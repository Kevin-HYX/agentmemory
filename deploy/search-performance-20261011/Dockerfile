FROM local/agentmemory:0.9.29-sharded-20260908
LABEL org.agentmemory.patch="search-performance-20261011"
COPY patch-search.mjs test-search-patch.mjs /opt/agentmemory/deploy/search-performance-20261011/
RUN node /opt/agentmemory/deploy/search-performance-20261011/test-search-patch.mjs /opt/agentmemory/node_modules/@agentmemory/agentmemory/dist/index.mjs \
 && node /opt/agentmemory/deploy/search-performance-20261011/patch-search.mjs \
 && node --check /opt/agentmemory/node_modules/@agentmemory/agentmemory/dist/index.mjs \
 && node /opt/agentmemory/deploy/search-performance-20261011/test-search-patch.mjs /opt/agentmemory/node_modules/@agentmemory/agentmemory/dist/index.mjs --expect-single
