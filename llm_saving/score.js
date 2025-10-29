// quality.js
import natural from "natural";

const tokenizer = new natural.WordTokenizer();

// Calcular ROUGE-L (simplificado: Longest Common Subsequence / len(ref))
function rougeL(ref, llm) {
  const refTokens = tokenizer.tokenize(ref.toLowerCase());
  const llmTokens = tokenizer.tokenize(llm.toLowerCase());

  const lcs = longestCommonSubsequence(refTokens, llmTokens);
  return lcs.length / refTokens.length;
}

// Función para Longest Common Subsequence (LCS)
function longestCommonSubsequence(a, b) {
  const m = a.length;
  const n = b.length;
  const dp = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));

  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      if (a[i - 1] === b[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1] + 1;
      } else {
        dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
      }
    }
  }

  // reconstrucción opcional: aquí devolvemos solo la longitud
  let i = m, j = n;
  const subseq = [];
  while (i > 0 && j > 0) {
    if (a[i - 1] === b[j - 1]) {
      subseq.unshift(a[i - 1]);
      i--; j--;
    } else if (dp[i - 1][j] > dp[i][j - 1]) {
      i--;
    } else {
      j--;
    }
  }
  return subseq;
}

export async function calculateQuality(respuestaRef, respuestaLLM) {
  return rougeL(respuestaRef, respuestaLLM);
}
