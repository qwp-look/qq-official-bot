FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY bot.js ./
ENV PORT=8080
EXPOSE 8080
CMD ["node", "bot.js"]
