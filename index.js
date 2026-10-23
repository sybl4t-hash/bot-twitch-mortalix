const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;

// Mini servidor web para mantener el servicio activo en la nube 24/7
app.get('/', (req, res) => {
  res.send('El bot de Twitch y los resúmenes están activos 🚀');
});

app.listen(PORT, () => {
  console.log(`Servidor web corriendo en el puerto ${PORT}`);
});

// --- LÓGICA DEL BOT DE TWITCH, REDIS Y GEMINI ---
const tmi = require('tmi.js');
const { Redis } = require('@upstash/redis');
const cron = require('node-cron');
const { GoogleGenAI } = require('@google/genai');

const redis = new Redis({
  url: process.env.UPSTASH_URL,
  token: process.env.UPSTASH_TOKEN,
});

if (!process.env.UPSTASH_URL || !process.env.UPSTASH_TOKEN) {
  console.error('Faltan UPSTASH_URL o UPSTASH_TOKEN en las variables de entorno.');
  process.exit(1);
}

// Inicializamos Gemini
const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

const client = new tmi.Client({
  channels: ['mortalix_zekhmet']
});

client.connect().catch(console.error);

function getStreamDateKey() {
  const now = new Date();
  if (now.getHours() < 14) {
    now.setDate(now.getDate() - 1);
  }
  return now.toISOString().split('T')[0];
}

// 1. Memoria Inmediata: Registro de mensajes en tiempo real
client.on('message', async (target, context, message, self) => {
  if (self) return;

  const username = context['display-name'] || context.username;
  const logEntry = `${username}: ${message}`;

  try {
    const streamDate = getStreamDateKey();
    const redisKey = `stream_chat:${streamDate}`;

    await redis.rpush(redisKey, logEntry);
    await redis.ltrim(redisKey, -2000, -1);
    await redis.expire(redisKey, 129600);

    console.log(`[Guardado - ${streamDate}] ${logEntry}`);
  } catch (err) {
    console.error('Error al guardar en Redis:', err);
  }
});

// 2 & 3. Resumen diario y consolidación a largo plazo
async function resumirStreamAnterior(streamDate) {
  try {
    const redisKey = `stream_chat:${streamDate}`;
    const mensajes = await redis.lrange(redisKey, 0, -1);

    if (!mensajes || mensajes.length === 0) {
      console.log(`[Resumen] No hay mensajes en el chat para la fecha: ${streamDate}`);
      return;
    }

    const chatTexto = mensajes.join('\n');
    console.log(`[Resumen] Generando resumen diario con gemini-3.8-flash para el ${streamDate}...`);

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
      model: 'gemini-3.8-flash',
      contents: promptDiario,
    });

    const resumenTexto = `[Stream ${streamDate}]:\n${response.text.trim()}`;

    // Guardamos en mediano plazo
    await redis.lpush('resumenes_mediano_plazo', resumenTexto);
    console.log(`[Resumen] Guardado en la memoria de mediano plazo.`);

    // Verificamos si alcanzamos 30 resúmenes para consolidar
    const totalResumenes = await redis.llen('resumenes_mediano_plazo');

    if (totalResumenes >= 30) {
      console.log(`[Consolidación] ¡Se alcanzaron 30 resúmenes! Creando memoria a largo plazo...`);

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
        model: 'gemini-3.8-flash',
        contents: promptLargoPlazo,
      });

      const macroResumen = `[Época/Bloque de 30 streams hasta ${streamDate}]:\n${macroResponse.text.trim()}`;

      // Guardamos el macro-resumen permanente y limpiamos la lista de mediano plazo
      await redis.lpush('memoria_largo_plazo', macroResumen);
      await redis.del('resumenes_mediano_plazo');

      console.log(`[Consolidación] Memoria a largo plazo actualizada y lista de 30 días reiniciada.`);
    }

  } catch (err) {
    console.error('[Resumen] Error en el proceso de resumen:', err);
  }
}

// Programación a las 2:05 PM (14:05)
cron.schedule('5 14 * * *', () => {
  const now = new Date();
  now.setDate(now.getDate() - 1);
  const fechaAProcesar = now.toISOString().split('T')[0];

  console.log(`[Cron] Activando tarea de resumen diario para: ${fechaAProcesar}`);
  resumirStreamAnterior(fechaAProcesar);
});