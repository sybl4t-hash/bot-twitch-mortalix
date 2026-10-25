const express = require('express');
const tmi = require('tmi.js');
const { Redis } = require('@upstash/redis');
const cron = require('node-cron');
const { GoogleGenAI } = require('@google/genai');

// ---------- Configuración ----------
const PORT = process.env.PORT || 3000;
const CHANNEL = process.env.CHANNEL_NAME || 'mortalix_zekhmet';
const STREAM_TZ = process.env.STREAM_TZ || 'UTC'; // debe ser igual que en el Worker
const CUTOFF = Number(process.env.STREAM_DAY_CUTOFF_HOUR ?? 14);
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-3.8-flash';

const { UPSTASH_URL, UPSTASH_TOKEN, GEMINI_API_KEY } = process.env;

if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  console.error('Faltan UPSTASH_URL o UPSTASH_TOKEN en las variables de entorno.');
  process.exit(1);
}

const redis = new Redis({ url: UPSTASH_URL, token: UPSTASH_TOKEN });
const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

// Usuarios cuyos mensajes no se guardan (bots)
const IGNORED_USERS = new Set([
  'nightbot', 'streamelements', 'streamlabs', 'moobot', 'fossabot', 'wizebot'
]);

// ---------- Fecha del "día de stream" (misma lógica que el Worker) ----------
function getStreamDateKey(now = new Date()) {
  const hour = Number(
    new Intl.DateTimeFormat('en-GB', { timeZone: STREAM_TZ, hour: '2-digit', hour12: false }).format(now)
  ) % 24;
  const ref = hour < CUTOFF ? new Date(now.getTime() - 24 * 60 * 60 * 1000) : now;
  return new Intl.DateTimeFormat('en-CA', { timeZone: STREAM_TZ }).format(ref); // YYYY-MM-DD
}

// ---------- Cliente de Twitch ----------
const client = new tmi.Client({ channels: [CHANNEL] });

client.on('connected', (addr, port) => {
  console.log(`[tmi] Conectado a ${addr}:${port}, escuchando #${CHANNEL}`);
});
client.on('disconnected', (reason) => {
  console.log('[tmi] Desconectado:', reason);
});

// ---------- Servidor web (mantiene vivo el servicio en Render) ----------
const app = express();

app.get('/', (req, res) => {
  res.send('El bot de Twitch y los resúmenes están activos 🚀');
});

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    twitch: client.readyState(),
    pendientesEnBuffer: buffer.length,
    streamKey: `stream_chat:${getStreamDateKey()}`
  });
});

app.listen(PORT, () => {
  console.log(`Servidor web corriendo en el puerto ${PORT}`);
});

// Auto-ping para que Render no duerma el servicio mientras esté despierto.
// Para despertarlo si ya se durmió, usa además UptimeRobot / cron-job.org.
const SELF_URL = process.env.RENDER_EXTERNAL_URL;
if (SELF_URL) {
  setInterval(() => {
    fetch(`${SELF_URL}/health`).catch((e) =>
      console.error('[keep-alive] Falló el auto-ping:', e.message)
    );
  }, 5 * 60 * 1000);
}

// ---------- 1. Memoria inmediata: guardado del chat por lotes ----------
let buffer = [];
let flushing = false;

client.on('message', (target, context, message, self) => {
  if (self) return;

  const login = String(context.username || '').toLowerCase();
  if (IGNORED_USERS.has(login)) return;   // ignora bots
  if (message.startsWith('!')) return;    // ignora comandos (evita guardar claves/URLs)

  const username = context['display-name'] || context.username;
  buffer.push(`${username}: ${message}`);

  if (buffer.length >= 100) flushBuffer();
});

async function flushBuffer() {
  if (flushing || buffer.length === 0) return;
  flushing = true;

  const batch = buffer;
  buffer = [];

  try {
    const key = `stream_chat:${getStreamDateKey()}`;
    const p = redis.pipeline();
    p.rpush(key, ...batch);
    p.ltrim(key, -2000, -1);
    p.expire(key, 129600);
    await p.exec();
    console.log(`[Guardado - ${key}] ${batch.length} mensajes`);
  } catch (err) {
    // Reintentar en el siguiente ciclo (con tope para no crecer sin límite)
    buffer = batch.concat(buffer).slice(-5000);
    console.error('[Redis] Error al guardar:', err);
  } finally {
    flushing = false;
  }
}

setInterval(flushBuffer, 10 * 1000);

// Al apagarse (Render manda SIGTERM) guardamos lo pendiente
async function shutdown(signal) {
  console.log(`[${signal}] Guardando buffer pendiente y cerrando...`);
  await flushBuffer();
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ---------- Arranque: verificar Redis y conectar a Twitch ----------
(async () => {
  try {
    await redis.ping();
    console.log(`[Redis] Conexión OK (${new URL(UPSTASH_URL).hostname})`);
  } catch (err) {
    console.error('[Redis] No se pudo conectar:', err);
  }
})();

client.connect().catch((err) => {
  console.error('[tmi] Error al conectar:', err);
  process.exit(1); // Render reinicia el servicio
});

// ---------- 2 & 3. Resumen diario y consolidación a largo plazo ----------
async function resumirStreamAnterior(streamDate) {
  try {
    const redisKey = `stream_chat:${streamDate}`;
    const mensajes = await redis.lrange(redisKey, 0, -1);

    if (!mensajes || mensajes.length === 0) {
      console.log(`[Resumen] No hay mensajes en el chat para la fecha: ${streamDate}`);
      return;
    }

    const chatTexto = mensajes.join('\n');
    console.log(`[Resumen] Generando resumen diario con ${GEMINI_MODEL} para el ${streamDate} (${mensajes.length} mensajes)...`);

    const promptDiario = `
Eres un asistente encargado de resumir streams de Twitch para mantener la memoria del canal.
Analiza el registro de chat del stream (${streamDate}) y genera un resumen estructurado y conciso.

Incluye:
- **Temas principales**: De qué se habló o jugó.
- **Momentos destacados**: Anécdotas, risas o chistes locales.
- **Contexto útil**: Eventos o datos a recordar.

Registro del chat:
${chatTexto}
`;

    const response = await ai.models.generateContent({
      model: GEMINI_MODEL,
      contents: promptDiario,
    });

    const resumenTexto = `[Stream ${streamDate}]:\n${response.text.trim()}`;

    await redis.lpush('resumenes_mediano_plazo', resumenTexto);
    console.log('[Resumen] Guardado en la memoria de mediano plazo.');

    const totalResumenes = await redis.llen('resumenes_mediano_plazo');

    if (totalResumenes >= 30) {
      console.log('[Consolidación] ¡Se alcanzaron 30 resúmenes! Creando memoria a largo plazo...');

      const ultimos30 = await redis.lrange('resumenes_mediano_plazo', 0, -1);
      const bloque30Texto = ultimos30.join('\n\n---\n\n');

      const promptLargoPlazo = `
Eres la memoria central de la comunidad de Twitch. Se te entregan los resúmenes individuales de los últimos 30 streams.
Sintetiza todos estos resúmenes en un único "Resumen de Época / Historia del Canal" muy conciso (máximo 400 palabras).

Destaca:
- Juegos/Series completadas o avanzadas en este periodo.
- Evolución de la comunidad, nuevos memes o chistes recurrentes consolidados.
- Eventos o hitos importantes del canal en este mes.

Resúmenes acumulados:
${bloque30Texto}
`;

      const macroResponse = await ai.models.generateContent({
        model: GEMINI_MODEL,
        contents: promptLargoPlazo,
      });

      const macroResumen = `[Época/Bloque de 30 streams hasta ${streamDate}]:\n${macroResponse.text.trim()}`;

      await redis.lpush('memoria_largo_plazo', macroResumen);
      await redis.del('resumenes_mediano_plazo');

      console.log('[Consolidación] Memoria a largo plazo actualizada y lista de 30 días reiniciada.');
    }
  } catch (err) {
    console.error('[Resumen] Error en el proceso de resumen:', err);
  }
}

// Corre a las CUTOFF:05 en STREAM_TZ (14:05 por defecto)
cron.schedule(`5 ${CUTOFF} * * *`, async () => {
  await flushBuffer(); // asegura que todo lo pendiente esté guardado
  const fechaAProcesar = getStreamDateKey(new Date(Date.now() - 24 * 60 * 60 * 1000));
  console.log(`[Cron] Activando tarea de resumen diario para: ${fechaAProcesar}`);
  resumirStreamAnterior(fechaAProcesar);
}, { timezone: STREAM_TZ });