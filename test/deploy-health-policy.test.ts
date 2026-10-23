import { describe, it, expect } from 'vitest';
import { isResponsiveHealth } from '../deploy/recovery-20261023/health-policy.mjs';

describe('部署健康策略', () => {
  const body = { service: 'agentmemory', health: { status: 'critical', alerts: ['cpu_critical_99%'], connectionState: 'connected', kvConnectivity: { status: 'ok' } } };
  it('CPU 忙碌导致 503 时仍认可正常的存储和工作连接', () => {
    expect(isResponsiveHealth(503, body)).toBe(true);
  });
  it('拒绝断开的存储、工作连接，以及未授权和缺失的响应', () => {
    expect(isResponsiveHealth(200, { ...body, health: { ...body.health, kvConnectivity: { status: 'error' } } })).toBe(false);
    expect(isResponsiveHealth(200, { ...body, health: { ...body.health, connectionState: 'disconnected' } })).toBe(false);
    expect(isResponsiveHealth(401, body)).toBe(false);
    expect(isResponsiveHealth(503, null)).toBe(false);
  });
});
