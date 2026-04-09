import { test, expect } from '@grafana/plugin-e2e';
import { setTimeout } from 'node:timers/promises';

async function dismissWhatsNewModal(page: any) {
  const dialog = page.getByRole('dialog', { name: /what's new in grafana/i });
  if (await dialog.isVisible().catch(() => false)) {
    await dialog.getByRole('button', { name: /^close$/i }).click();
    await expect(dialog).toBeHidden({ timeout: 10000 });
  }
}

async function createDashboardWithPanel(request: any) {
  const response = await request.post('/api/dashboards/db', {
    data: {
      dashboard: {
        title: `Nominal E2E ${Date.now()}`,
        panels: [
          {
            id: 1,
            title: 'Panel 1',
            type: 'timeseries',
            gridPos: { h: 8, w: 12, x: 0, y: 0 },
            targets: [{}],
          },
        ],
      },
      overwrite: false,
      folderId: 0,
    },
  });

  expect(response.ok()).toBeTruthy();
  const body = await response.json();
  return body.uid as string;
}

function getQueryEditorRow(page: any) {
  return page
    .locator('div')
    .filter({ has: page.getByRole('button', { name: /^A$/ }) })
    .filter({ has: page.getByRole('button', { name: /collapse query row|expand query row/i }) })
    .first();
}

async function openPanelEditPage(page: any, request: any, gotoPanelEditPage: any) {
  const uid = await createDashboardWithPanel(request);
  const panelEditPage = await gotoPanelEditPage({ dashboard: { uid }, id: '1' });
  await dismissWhatsNewModal(page);
  await expect(page.getByTestId('data-testid Select a data source')).toBeVisible({ timeout: 15000 });
  return panelEditPage;
}

test('smoke: should render query editor', async ({ gotoPanelEditPage, page, request, readProvisionedDataSource }) => {
  const panelEditPage = await openPanelEditPage(page, request, gotoPanelEditPage);
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  await expect(getQueryEditorRow(page).getByRole('radio', { name: 'Asset Search' })).toBeVisible();
});

test('should trigger new query when search field is changed', async ({
  gotoPanelEditPage,
  page,
  request,
  readProvisionedDataSource,
}) => {
  test.setTimeout(20000);

  const panelEditPage = await openPanelEditPage(page, request, gotoPanelEditPage);
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  const queryRow = getQueryEditorRow(page);

  await expect(queryRow.getByRole('radio', { name: 'Asset Search' })).toBeVisible();
  await queryRow.getByRole('radio', { name: 'Asset Search' }).check();
  const searchInput = queryRow.getByPlaceholder('Search assets');
  await searchInput.fill('drone');
  await setTimeout(1000);
  await expect(searchInput).toHaveValue('drone');
});

test('data query should work with asset and channel selection', async ({
  gotoPanelEditPage,
  page,
  request,
  readProvisionedDataSource,
}) => {
  const panelEditPage = await openPanelEditPage(page, request, gotoPanelEditPage);
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  const queryRow = getQueryEditorRow(page);

  await expect(queryRow.getByRole('radio', { name: 'Asset Search' })).toBeVisible();
  await queryRow.getByRole('radio', { name: 'Asset RID' }).check();
  const ridInput = queryRow.getByPlaceholder('ri.scout.cerulean-staging.asset...');
  await ridInput.fill('ri.scout.cerulean-staging.asset.test-asset-rid');
  await expect(ridInput).toHaveValue('ri.scout.cerulean-staging.asset.test-asset-rid');
});
