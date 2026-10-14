import { test, expect } from '@grafana/plugin-e2e';
import { setTimeout } from 'node:timers/promises';
import { ASSET_RID_FALLBACK_LABEL } from '../src/components/queryBuilder/assetReconcile';

const ASSET_PLACEHOLDER = 'Search assets or paste a RID...';
const DATA_SCOPE_PLACEHOLDER = 'Choose scope or use $variable...';
const TEST_ASSET_RID = 'ri.scout.cerulean-staging.asset.test-asset-rid';

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

async function getQueryEditorRow(panelEditPage: any, page: any) {
  const builtInRow = panelEditPage.getQueryEditorRow('A');
  const builtInAssetInput = builtInRow.getByPlaceholder(ASSET_PLACEHOLDER, { exact: true });

  if (await builtInAssetInput.isVisible().catch(() => false)) {
    return builtInRow;
  }

  return page
    .locator('div')
    .filter({ has: page.getByRole('button', { name: /^A$/ }) })
    .filter({ has: page.getByRole('button', { name: /collapse query row|expand query row/i }) })
    .first();
}

// The pickers load options asynchronously; Enter before the typed value's option
// renders selects nothing and clears the input.
async function commitTypedValue(page: any, input: any, value: string) {
  await input.fill(value);
  await expect(page.getByRole('option', { name: value })).toBeVisible();
  await input.press('Enter');
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
  const queryRow = await getQueryEditorRow(panelEditPage, page);
  await expect(queryRow.getByPlaceholder(ASSET_PLACEHOLDER, { exact: true })).toBeVisible();
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
  const queryRow = await getQueryEditorRow(panelEditPage, page);

  const searchInput = queryRow.getByPlaceholder(ASSET_PLACEHOLDER, { exact: true });
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
  const queryRow = await getQueryEditorRow(panelEditPage, page);

  const assetInput = queryRow.getByPlaceholder(ASSET_PLACEHOLDER, { exact: true });
  await expect(assetInput).toBeVisible();
  await commitTypedValue(page, assetInput, TEST_ASSET_RID);

  // Data scope only renders for a committed assetRid; the summary RID appears
  // once the by-RID fetch settles (real or fallback asset).
  await expect(queryRow.getByPlaceholder(DATA_SCOPE_PLACEHOLDER, { exact: true })).toBeVisible();
  await expect(queryRow.getByText(TEST_ASSET_RID)).toBeVisible();
});

const TEST_DATASET_RID = 'ri.catalog.cerulean-staging.dataset.test-dataset-rid';

// The test RIDs never resolve, so each picker settles on its unresolved label:
// the dataset picker keeps showing the RID, the asset picker its fallback label.
for (const { source, previousRid, shownLabel, wrongRid, message } of [
  {
    source: 'Dataset',
    previousRid: TEST_DATASET_RID,
    shownLabel: TEST_DATASET_RID,
    wrongRid: TEST_ASSET_RID,
    message: 'Not a dataset RID',
  },
  {
    source: 'Asset',
    previousRid: TEST_ASSET_RID,
    shownLabel: ASSET_RID_FALLBACK_LABEL,
    wrongRid: TEST_DATASET_RID,
    message: 'Not an asset RID',
  },
]) {
  test(`${source} picker rejects a pasted RID of the other type and keeps its selection`, async ({
    gotoPanelEditPage,
    page,
    request,
    readProvisionedDataSource,
  }) => {
    const panelEditPage = await openPanelEditPage(page, request, gotoPanelEditPage);
    const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
    await panelEditPage.datasource.set(ds.name);
    const queryRow = await getQueryEditorRow(panelEditPage, page);
    await queryRow
      .getByRole('radiogroup', { name: 'Source' })
      .getByRole('radio', { name: source, exact: true })
      .click();

    // By id: while its menu is open, the Combobox swaps the placeholder for the selected label.
    const input = queryRow.locator(`#nominal-query-${source.toLowerCase()}-picker`);
    await commitTypedValue(page, input, previousRid);
    await expect(input).toHaveValue(shownLabel);

    await commitTypedValue(page, input, wrongRid);
    await expect(queryRow.getByText(message, { exact: true })).toBeVisible();
    await input.press('Escape');
    await expect(input).toHaveValue(shownLabel);
  });
}
