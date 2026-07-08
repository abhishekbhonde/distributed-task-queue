# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM node:18-alpine AS build

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

# Pass a dummy URL during build if needed (build time override)
ENV NEXT_PUBLIC_API_URL=http://localhost:8080
RUN npm run build

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM node:18-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --production

# Copy built app and assets
COPY --from=build /app/.next ./.next
COPY --from=build /app/next.config.mjs ./next.config.mjs

EXPOSE 3000

ENV PORT=3000
ENV NODE_ENV=production

CMD ["npm", "start"]
