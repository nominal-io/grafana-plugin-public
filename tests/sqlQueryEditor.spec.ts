import { test, expect } from '@grafana/plugin-e2e';

async function dismissWhatsNewModal(page: any) {
  const dialog = page.getByRole('dialog', { name: /what's new in grafana/i });
  if (await dialog.isVisible().catch(() => false)) {
    await dialog.getByRole('button', { name: /^close$/i }).click();
    await expect(dialog).toBeHidden({ timeout: 10000 });
  }
}

test('edits a SQL table query', async ({ gotoPanelEditPage, page, request, readProvisionedDataSource }) => {
  const provisioned = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  const existingResponse = await request.get(`/api/datasources/name/${encodeURIComponent(provisioned.name)}`);
  expect(existingResponse.ok()).toBeTruthy();
  const existing = await existingResponse.json();
  const updateResponse = await request.put(`/api/datasources/uid/${existing.uid}`, {
    data: {
      ...existing,
      jsonData: { ...existing.jsonData, enableSql: true },
    },
  });
  expect(updateResponse.ok()).toBeTruthy();

  const dashboardResponse = await request.post('/api/dashboards/db', {
    data: {
      dashboard: {
        title: `Nominal SQL E2E ${Date.now()}`,
        panels: [{ id: 1, title: 'Panel 1', type: 'table', gridPos: { h: 8, w: 12, x: 0, y: 0 }, targets: [{}] }],
      },
      overwrite: false,
      folderId: 0,
    },
  });
  expect(dashboardResponse.ok()).toBeTruthy();
  const dashboard = await dashboardResponse.json();
  const panelEditPage = await gotoPanelEditPage({ dashboard: { uid: dashboard.uid }, id: '1' });
  await dismissWhatsNewModal(page);
  await panelEditPage.datasource.set(provisioned.name);

  await page.getByRole('radio', { name: /^sql$/i }).click();
  const editor = page.getByTestId('sql-code-editor').locator('textarea');
  await editor.fill('SELECT 1 AS v');
  await editor.blur();
  await page.getByRole('radio', { name: /^table$/i }).click();
});
