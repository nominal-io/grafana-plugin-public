import { test, expect } from '@grafana/plugin-e2e';

// The browser tests isolate the editor from Nominal availability. Go tests cover
// the gRPC/Arrow contract; the opt-in live test verifies actual SQL execution.
const MOCK_VALUES: Record<string, number> = { A: 42, B: 7, C: 9 };

test('mixes Compute and SQL on one datasource and keeps SQL edits per query', async ({
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
  const lastRun = (refId: string) => [...executed].reverse().find((query) => query.refId === refId);
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
                name: `${query.refId} result`,
                refId: query.refId,
                fields: [{ name: 'value', type: 'number', typeInfo: { frame: 'float64' } }],
              },
              data: { values: [[MOCK_VALUES[query.refId]]] },
            },
          ],
        };
      }
      await route.fulfill({ json: { results } });
    });
    const target = { datasource: { type: source.type, uid: datasource.uid } };
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
              datasource: target.datasource,
              targets: [
                { ...target, refId: 'A', queryType: 'sql', rawSql: '', format: 'table' },
                {
                  ...target,
                  refId: 'B',
                  queryType: 'timeShift',
                  assetRid, channel: 'temperature', channelDataType: 'numeric', dataScopeName: 'default',
                },
                { ...target, refId: 'C', queryType: 'sql', rawSql: 'SELECT 9 AS value', format: 'table' },
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
    const panelEditPage = await gotoPanelEditPage({ dashboard: { uid: dashboard.uid }, id: '1' });
    const dialog = page.getByRole('dialog', { name: /what's new in grafana/i });
    if (await dialog.isVisible().catch(() => false)) {
      await dialog.getByRole('button', { name: /^close$/i }).click();
      await expect(dialog).toBeHidden({ timeout: 10000 });
    }

    const rowA = panelEditPage.getQueryEditorRow('A');
    const editor = rowA.getByTestId('sql-code-editor').locator('textarea');
    await editor.click();
    await editor.press('Control+A');
    await editor.pressSequentially('SELECT 42 AS value');
    // Query C mounts a second SQL editor, so the shortcut must stay bound to query A.
    await editor.press('Control+Enter');
    await expect.poll(() => lastRun('A')?.rawSql).toBe('SELECT 42 AS value');
    expect(lastRun('A')).toMatchObject({ queryType: 'sql', format: 'table' });
    expect(lastRun('B')).toMatchObject({ queryType: 'timeShift' });
    expect(lastRun('C')).toMatchObject({ queryType: 'sql', rawSql: 'SELECT 9 AS value' });
    await expect(page.getByText('42', { exact: true }).first()).toBeVisible();

    // Leave an uncommitted edit in Monaco before switching APIs.
    await editor.press('Control+A');
    await editor.pressSequentially('SELECT 43 AS value');
    await rowA.getByRole('radio', { name: /^Compute$/ }).click();
    await expect(rowA.getByTestId('sql-code-editor')).toHaveCount(0);
    await rowA.getByRole('radio', { name: /^SQL$/ }).click();
    await expect(rowA.getByTestId('sql-code-editor')).toContainText('SELECT 43 AS value');
    const count = executed.length;
    await editor.press('Control+Enter');
    await expect.poll(() => executed.length).toBeGreaterThan(count);
    expect(lastRun('A')?.rawSql).toBe('SELECT 43 AS value');
    expect(lastRun('C')?.rawSql).toBe('SELECT 9 AS value');
  } finally {
    if (dashboardUid) {
      await request.delete(`/api/dashboards/uid/${dashboardUid}`);
    }
    await request.delete(`/api/datasources/uid/${datasource.uid}`);
  }
});
