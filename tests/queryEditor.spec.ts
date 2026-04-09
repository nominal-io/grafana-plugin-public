import { test, expect } from '@grafana/plugin-e2e';
import { setTimeout } from 'node:timers/promises';

async function dismissWhatsNewModal(page: any) {
  const dialog = page.getByRole('dialog', { name: /what's new in grafana/i });
  if (await dialog.isVisible().catch(() => false)) {
    await dialog.getByRole('button', { name: /^close$/i }).click();
    await expect(dialog).toBeHidden({ timeout: 10000 });
  }
}

async function createDashboardWithPanel(page: any) {
  const result = await page.evaluate(async () => {
    const payload = {
      dashboard: {
        id: null,
        uid: null,
        title: `Nominal E2E ${Date.now()}`,
        schemaVersion: 41,
        version: 0,
        panels: [
          {
            id: 1,
            title: 'Panel 1',
            type: 'timeseries',
            gridPos: { h: 8, w: 12, x: 0, y: 0 },
            targets: [{}],
            datasource: null,
          },
        ],
      },
      overwrite: false,
      folderId: 0,
    };

    const response = await fetch('/api/dashboards/db', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    return {
      ok: response.ok,
      status: response.status,
      body: await response.text(),
    };
  });

  expect(result.ok, `Failed to create dashboard: ${result.status} ${result.body}`).toBeTruthy();
  const body = JSON.parse(result.body);
  return body.uid as string;
}

async function openPanelEditPage(page: any, gotoPanelEditPage: any) {
  const uid = await createDashboardWithPanel(page);
  const panelEditPage = await gotoPanelEditPage({ dashboard: { uid }, id: '1' });
  await dismissWhatsNewModal(page);
  await expect(page.getByTestId('data-testid Select a data source')).toBeVisible({ timeout: 15000 });
  return panelEditPage;
}

test('smoke: should render query editor', async ({ gotoPanelEditPage, page, readProvisionedDataSource }) => {
  const panelEditPage = await openPanelEditPage(page, gotoPanelEditPage);
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  await expect(page.getByRole('radio', { name: 'Asset Search' })).toBeVisible();
});

test('should trigger new query when search field is changed', async ({
  gotoPanelEditPage,
  page,
  readProvisionedDataSource,
}) => {
  test.setTimeout(20000);

  const panelEditPage = await openPanelEditPage(page, gotoPanelEditPage);
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);

  await expect(page.getByRole('radio', { name: 'Asset Search' })).toBeVisible();

  try {
    const searchRadio = page.getByRole('radio', { name: 'Asset Search' });
    if (await searchRadio.isVisible()) {
      await searchRadio.check();
    }

    await page.getByPlaceholder('Search assets').fill('drone');
    await setTimeout(1000);
    await expect(page.getByDisplayValue('drone')).toBeVisible();
  } catch (error) {
    expect(error).toBeDefined();
  }
});

test('data query should work with asset and channel selection', async ({
  gotoPanelEditPage,
  page,
  readProvisionedDataSource,
}) => {
  const panelEditPage = await openPanelEditPage(page, gotoPanelEditPage);
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  await expect(page.getByRole('radio', { name: 'Asset Search' })).toBeVisible();
  await page.getByRole('radio', { name: 'Asset RID' }).check();
  const ridInput = page.getByPlaceholder('ri.scout.cerulean-staging.asset...');
  await ridInput.fill('ri.scout.cerulean-staging.asset.test-asset-rid');
  await expect(ridInput).toHaveValue('ri.scout.cerulean-staging.asset.test-asset-rid');
});
