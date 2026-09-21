import { test, expect } from '@grafana/plugin-e2e';

// The browser tests isolate the editor from Nominal availability. Go tests cover
// the HTTP/Arrow contract; the opt-in live test verifies actual SQL execution.
test('builds SQL, runs it, and preserves edits when changing editor modes', async ({
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
      jsonData: { queryApi: 'sql' },
    },
  });
  expect(createResponse.ok()).toBeTruthy();
  const { datasource } = await createResponse.json();
  let dashboardUid: string | undefined;
  const executed: any[] = [];
  try {
    await page.route('**/resources/sql/datasets', (route) =>
      route.fulfill({ json: [{ label: 'Engine test', value: 'dataset-1' }] })
    );
    await page.route('**/resources/sql/channels', async (route) => {
      expect(route.request().postDataJSON().datasetRid).toBe('dataset-1');
      await route.fulfill({ json: [{ label: 'temperature', value: 'temperature' }] });
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
                  queryType: 'sql',
                  sqlEditorMode: 'builder',
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

    await page.getByTestId('sql-dataset-picker').click();
    await page.getByRole('option', { name: /Engine test/ }).click();
    await page.getByRole('combobox', { name: 'Search channels...' }).click();
    await page.getByRole('option', { name: /temperature/ }).click();
    await page.keyboard.press('Escape');
    await page.getByRole('button', { name: /^Run query$/ }).click();
    await expect.poll(() => executed.length).toBeGreaterThan(0);
    expect(executed.at(-1)).toMatchObject({
      queryType: 'sql',
      format: 'table',
      rawSql: expect.stringContaining("dataset_rid = 'dataset-1'"),
    });
    expect(executed.at(-1).rawSql).toContain("channel IN ('temperature')");
    await expect(page.getByText('42', { exact: true }).first()).toBeVisible();

    await page.getByRole('radio', { name: /^Code$/ }).click();
    const editor = page.getByTestId('sql-code-editor').locator('textarea');
    await editor.click();
    await editor.press('Control+A');
    await editor.pressSequentially('SELECT 42 AS value');
    await editor.press('Control+Enter');
    await expect.poll(() => executed.at(-1)?.rawSql).toBe('SELECT 42 AS value');
    await page.getByRole('radio', { name: /^Builder$/ }).click();
    await expect(page.getByText('Replace edited SQL with the builder query?')).toBeVisible();
    await page.getByRole('button', { name: 'Keep code' }).click();
    await expect(page.getByTestId('sql-code-editor')).toContainText('SELECT 42 AS value');
  } finally {
    if (dashboardUid) {
      await request.delete(`/api/dashboards/uid/${dashboardUid}`);
    }
    await request.delete(`/api/datasources/uid/${datasource.uid}`);
  }
});
