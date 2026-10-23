FROM iiidev/iii:0.22.1 AS engine
FROM node:24-bookworm-slim
ARG HTTPS_PROXY
RUN node -e "const f=require('fs');f.mkdirSync('/etc/ssl/certs',{recursive:true});f.writeFileSync('/etc/ssl/certs/ca-certificates.crt',require('tls').rootCertificates.join('\n'))" \
 && sed -i 's,http://deb.debian.org,https://deb.debian.org,g' /etc/apt/sources.list.d/debian.sources \
 && apt-get -o Acquire::https::Proxy="${HTTPS_PROXY}" update -qq \
 && apt-get -o Acquire::https::Proxy="${HTTPS_PROXY}" install -y --no-install-recommends ca-certificates git \
 && rm -rf /var/lib/apt/lists/*
COPY --from=engine /app/iii /usr/local/bin/iii
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --omit=optional --ignore-scripts --no-audit --no-fund
RUN npm install --prefix /opt/tokenizer --omit=optional --ignore-scripts --no-audit --no-fund @node-rs/jieba@2.0.3 @node-rs/jieba-linux-x64-gnu@2.0.3 \
 && mkdir -p /app/node_modules/@node-rs \
 && cp -R /opt/tokenizer/node_modules/@node-rs/. /app/node_modules/@node-rs/ \
 && node -e "require('@node-rs/jieba')" \
 && rm -rf /opt/tokenizer
COPY dist ./dist
COPY deploy/recovery-20261023/supervisor.mjs ./supervisor.mjs
COPY deploy/recovery-20261023/health-policy.mjs ./health-policy.mjs
COPY deploy/recovery-20261023/iii-config.yaml ./iii-config.yaml
ENV MALLOC_ARENA_MAX=2 AGENTMEMORY_DATA_DIR=/data NODE_OPTIONS=--max-old-space-size=4096
USER node
HEALTHCHECK --interval=30s --timeout=6s --start-period=300s --retries=3 CMD node -e "fetch('http://127.0.0.1:3111/agentmemory/livez',{signal:AbortSignal.timeout(5000)}).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "/app/supervisor.mjs"]
