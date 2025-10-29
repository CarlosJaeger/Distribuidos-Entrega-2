FROM node:20-slim

WORKDIR /app

# Copia el package.json y package-lock.json desde la carpeta actual (llm/ o success/)
COPY package*.json ./

# Instala dependencias de producción
RUN npm install --only=production

# Copia el resto del código del servicio
COPY . .

# Arranca la app
CMD ["node", "index.js"]
