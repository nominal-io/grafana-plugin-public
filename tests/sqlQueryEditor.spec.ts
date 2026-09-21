import { test, expect } from '@grafana/plugin-e2e';

// The browser tests isolate the editor from Nominal availability. Go tests cover
// the HTTP/Arrow contract; the opt-in live test verifies actual SQL execution.
test('mixes Compute and SQL on one datasource and preserves SQL while switching', async ({
  gotoPanelEditPage,
  page,
  request,
  readProvisionedDataSource,
}) => {
  test.setTimeout(60000);
  const provisioned = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  const sourceResponse = await request.get(`/api/datasources/name/${encodeURIComponent(provisioned.name)}`);
  expect(sourceResponse.ok()).toBeTruthy();
  const source = await sourceResponse.json();
  const createResponse = await request.post('/api/datasources', {
    data: {
      name: `Nominal SQL E2E ${Date.now()}`,
      type: source.type,
      access: 'proxy',
      jsonData: {},
    },
  });
  expect(createResponse.ok()).toBeTruthy();
  const { datasource } = await createResponse.json();
  let dashboardUid: string | undefined;
  const executed: any[] = [];
  try {
    const assetRid = 'ri.scout.main.asset.e2e';
    await page.route(`**/api/datasources/uid/${datasource.uid}/resources/**`, async (route) => {
      const path = new URL(route.request().url()).pathname;
      const asset = { rid: assetRid, title: 'E2E asset', labels: [], dataScopes: [{ dataScopeName: 'default', dataSource: { type: 'logSet', logSet: 'ri.logset.main.log-set.e2e' } }] };
      await route.fulfill({ json: path.endsWith('/assets-by-rid') ? { [assetRid]: asset }
        : path.endsWith('/channels') ? { channels: [{ name: 'temperature', dataType: 'numeric' }] }
        : { results: [asset] } });
    });
    await page.route('**/api/ds/query*', async (route) => {
      const body = route.request().postDataJSON();
      const queries = body.queries.filter((query: any) => query.datasource?.uid === datasource.uid);
      if (!queries.length) {
        return route.continue();
      }
      const results: Record<string, unknown> = {};
      for (const query of queries) {
        executed.push(query);
        results[query.refId] = {
          status: 200,
          frames: [
            {
              schema: {
                name: 'SQL result',
                refId: query.refId,
                fields: [{ name: 'value', type: 'number', typeInfo: { frame: 'float64' } }],
              },
              data: { values: [[42]] },
            },
          ],
        };
      }
      await route.fulfill({ json: { results } });
    });
    const dashboardResponse = await request.post('/api/dashboards/db', {
      data: {
        dashboard: {
          title: `Nominal SQL E2E ${Date.now()}`,
          panels: [
            {
              id: 1,
              title: 'SQL result',
              type: 'table',
              gridPos: { h: 8, w: 12, x: 0, y: 0 },
              datasource: { type: source.type, uid: datasource.uid },
              targets: [
                {
                  refId: 'A',
                  datasource: { type: source.type, uid: datasource.uid },
                  queryType: 'timeShift',
                  assetRid, channel: 'temperature', channelDataType: 'numeric', dataScopeName: 'default',
                },
                {
                  refId: 'B',
                  datasource: { type: source.type, uid: datasource.uid },
                  queryType: 'sql',
                  rawSql: '',
                  format: 'table',
                },
              ],
            },
          ],
        },
        overwrite: false,
      },
    });
    expect(dashboardResponse.ok()).toBeTruthy();
    const dashboard = await dashboardResponse.json();
    dashboardUid = dashboard.uid;
    await gotoPanelEditPage({ dashboard: { uid: dashboard.uid }, id: '1' });
    const dialog = page.getByRole('dialog', { name: /what's new in grafana/i });
    if (await dialog.isVisible().catch(() => false)) {
      await dialog.getByRole('button', { name: /^close$/i }).click();
    }

    await expect(page.getByRole('radio', { name: /^Builder$/ })).toHaveCount(0);
    const editor = page.getByTestId('sql-code-editor').locator('textarea');
    await editor.click();
    await editor.press('Control+A');
    await editor.pressSequentially('SELECT 42 AS value');
    await editor.press('Control+Enter');
    await expect.poll(() => executed.at(-1)?.rawSql).toBe('SELECT 42 AS value');
    expect(executed.at(-1)).toMatchObject({ queryType: 'sql', format: 'table' });
    await expect(page.getByText('42', { exact: true }).first()).toBeVisible();
    expect(executed.some((query) => query.refId === 'A' && query.queryType === 'timeShift')).toBeTruthy();
    expect(executed.some((query) => query.refId === 'B' && query.queryType === 'sql')).toBeTruthy();
    // Leave an uncommitted edit in Monaco before switching APIs.
    await editor.press('Control+A');
    await editor.pressSequentially('SELECT 43 AS value');
    await page.getByRole('radio', { name: /^Compute$/ }).last().click();
    await expect(page.getByTestId('sql-code-editor')).toHaveCount(0);
    await page.getByRole('radio', { name: /^SQL$/ }).last().click();
    await expect(page.getByTestId('sql-code-editor')).toContainText('SELECT 43 AS value');
    const count = executed.length;
    await editor.press('Control+Enter');
    await expect.poll(() => executed.length).toBeGreaterThan(count);
    expect(executed.at(-1).rawSql).toBe('SELECT 43 AS value');
  } finally {
    if (dashboardUid) {
      await request.delete(`/api/dashboards/uid/${dashboardUid}`);
    }
    await request.delete(`/api/datasources/uid/${datasource.uid}`);
  }
});
