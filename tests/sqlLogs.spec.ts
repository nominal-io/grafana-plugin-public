import { test, expect } from '@grafana/plugin-e2e';

const FROM = Date.UTC(2024, 0, 1);
const TO = FROM + 10 * 60 * 1000;

// The backend sends nanos only for pages that hold a sub-millisecond timestamp.
const logsResponse = (rows: Array<[number, string]>, withNanos: boolean) => ({
  results: {
    A: {
      status: 200,
      frames: [
        {
          schema: {
            refId: 'A',
            meta: { type: 'log-lines', typeVersion: [0, 0], preferredVisualisationType: 'logs' },
            fields: [
              { name: 'timestamp', type: 'time', typeInfo: { frame: 'time.Time' } },
              { name: 'body', type: 'string', typeInfo: { frame: 'string' } },
            ],
          },
          data: {
            values: [rows.map(([time]) => time), rows.map(([, body]) => body)],
            ...(withNanos && { nanos: [rows.map(() => 500000), null] }),
          },
        },
      ],
    },
  },
});

test('loads an older page of SQL logs in Explore when only the first page has nanoseconds', async ({
  createDataSource,
  page,
  request,
}) => {
  test.setTimeout(60000);
  const ds = await createDataSource({ type: 'nominal-nominalds-datasource', name: `Nominal logs E2E ${Date.now()}` });
  try {
    await page.route('**/api/ds/query*', async (route) => {
      const firstPage = Number(route.request().postDataJSON().to) === TO;
      const rows: Array<[number, string]> = firstPage
        ? Array.from({ length: 100 }, (_, i) => [TO - i * 1000, `newer line ${i}`])
        : [[FROM + 60000, 'oldest line']];
      await route.fulfill({ json: logsResponse(rows, firstPage) });
    });
    const query = { refId: 'A', datasource: { type: ds.type, uid: ds.uid }, queryType: 'sql', format: 'logs', rawSql: 'SELECT 1' };
    const panes = { a: { datasource: ds.uid, queries: [query], range: { from: String(FROM), to: String(TO) } } };
    await page.goto(`/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify(panes))}`);
    const newest = page.getByText('newer line 0', { exact: true });
    await expect(newest).toBeVisible();
    const dialog = page.getByRole('dialog', { name: /what's new in grafana/i });
    if (await dialog.isVisible().catch(() => false)) {
      await dialog.getByRole('button', { name: /^close$/i }).click();
      await expect(dialog).toBeHidden({ timeout: 10000 });
    }

    // Grafana 12.1 ignores wheel events under 500 ms apart at the end of the list, so each try waits a second.
    await newest.hover();
    await expect(async () => {
      await page.mouse.wheel(0, 3000);
      await expect(page.getByText('oldest line')).toBeVisible({ timeout: 1000 });
    }).toPass({ timeout: 30000 });
  } finally {
    await request.delete(`/api/datasources/uid/${ds.uid}`);
  }
});
