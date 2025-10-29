import OpenAI from 'openai';

const openai = new OpenAI({ apiKey: OPENAI_API_KEY});
const MODEL = process.env.OPENAI_MODEL || "gpt-3.5-turbo";

export async function getLLMResponse(prompt, maxRetries = 3) {
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      const resp = await openai.chat.completions.create({
        model: MODEL,
        messages: [{ role: "user", content: prompt }],
        max_tokens: 200
      });

      const text = resp.choices?.[0]?.message?.content?.trim() ?? "";
      return { text, retries: attempt }; // respuesta + nº de reintentos
    } catch (err) {
      attempt++;
      const msg = (err?.message || "").toLowerCase();

      // 🚨 Caso QUOTA: pausa 1 hora
      if (msg.includes("quota")) {
        console.warn("⚠️ [Worker] Límite de cuota alcanzado. Esperando 1 hora...");
        await new Promise(r => setTimeout(r, 60 * 60 * 1000));
        continue;
      }

      // 🚨 Otros errores: backoff exponencial
      if (attempt < maxRetries) {
        const delay = Math.min(2 ** attempt * 1000, 60_000);
        console.warn(`⚠️ [Worker] Error en intento ${attempt}, reintentando en ${delay/1000}s`);
        await new Promise(r => setTimeout(r, delay));
      } else {
        throw new Error(`LLM falló tras ${attempt} intentos: ${err.message}`);
      }
    }
  }
}



