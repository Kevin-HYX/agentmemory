export function isResponsiveHealth(status, body) {
  return (status === 200 || status === 503)
    && body?.service === 'agentmemory'
    && body.health?.kvConnectivity?.status === 'ok'
    && body.health?.connectionState === 'connected';
}
