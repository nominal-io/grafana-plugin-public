import React from 'react';
import { css } from '@emotion/css';
import { Button, Stack, useStyles2 } from '@grafana/ui';
import { GrafanaTheme2, QueryEditorHelpProps } from '@grafana/data';

import { DEFAULT_AGGREGATIONS, DEFAULT_SQL, NominalQuery } from '../types';

const getStyles = (theme: GrafanaTheme2) => ({
  root: css({
    maxWidth: 720,
  }),
  intro: css({
    color: theme.colors.text.secondary,
    marginBottom: theme.spacing(2),
  }),
  section: css({
    marginBottom: theme.spacing(2),
  }),
  title: css({
    color: theme.colors.text.primary,
    fontSize: theme.typography.h5.fontSize,
    fontWeight: theme.typography.fontWeightMedium,
    margin: `0 0 ${theme.spacing(1)} 0`,
  }),
  text: css({
    color: theme.colors.text.secondary,
    margin: 0,
  }),
  code: css({
    background: theme.colors.background.secondary,
    border: `1px solid ${theme.colors.border.weak}`,
    borderRadius: theme.shape.radius.default,
    color: theme.colors.text.primary,
    display: 'inline-block',
    fontFamily: theme.typography.fontFamilyMonospace,
    marginTop: theme.spacing(1),
    padding: theme.spacing(0.5, 1),
  }),
  codeBlock: css({
    background: theme.colors.background.secondary,
    border: `1px solid ${theme.colors.border.weak}`,
    borderRadius: theme.shape.radius.default,
    color: theme.colors.text.primary,
    display: 'block',
    fontFamily: theme.typography.fontFamilyMonospace,
    marginTop: theme.spacing(1),
    padding: theme.spacing(1),
    whiteSpace: 'pre-wrap',
  }),
  table: css({
    borderCollapse: 'collapse',
    color: theme.colors.text.secondary,
    marginTop: theme.spacing(1),
    width: '100%',
    '& th, & td': {
      border: `1px solid ${theme.colors.border.weak}`,
      padding: theme.spacing(0.5, 1),
      textAlign: 'left',
    },
    '& th': {
      color: theme.colors.text.primary,
    },
  }),
});

const timeSeriesExample: NominalQuery = {
  refId: 'A',
  assetRid: '${asset}',
  dataScopeName: '${datascope}',
  channel: '${channel}',
  queryType: 'decimation',
  buckets: 1000,
  aggregations: [...DEFAULT_AGGREGATIONS],
};

export function QueryEditorHelp({ onClickExample }: QueryEditorHelpProps<NominalQuery>) {
  const styles = useStyles2(getStyles);

  return (
    <div className={styles.root}>
      <p className={styles.intro}>
        Build Nominal queries by choosing an asset, data scope, and channel. Dashboard variables can be used in each field.
      </p>

      <div className={styles.section}>
        <h3 className={styles.title}>Time series query</h3>
        <p className={styles.text}>Use a numeric channel with decimation for dashboards and alert rules.</p>
        <Stack alignItems="center" gap={1}>
          <Button size="sm" variant="secondary" onClick={() => onClickExample(timeSeriesExample)}>
            Use example
          </Button>
          <code className={styles.code}>{'${asset} / ${datascope} / ${channel}'}</code>
        </Stack>
      </div>

      <div className={styles.section}>
        <h3 className={styles.title}>Variable queries</h3>
        <p className={styles.text}>Create chained variables with these query patterns.</p>
        <code className={styles.code}>assets</code>
        <br />
        <code className={styles.code}>{'datascopes(${asset})'}</code>
        <br />
        <code className={styles.code}>{'channels(${asset}, ${datascope})'}</code>
      </div>

      <div className={styles.section}>
        <h3 className={styles.title}>SQL queries</h3>
        <p className={styles.text}>Choose Query API: SQL in the data source settings. Builder creates numeric time-series queries from a dataset, channels, aggregation and time bucket. Code supports custom SQL, joins and other tables.</p>
        <table className={styles.table}>
          <thead>
            <tr><th>Macro</th><th>Expands to</th></tr>
          </thead>
          <tbody>
            <tr><td>$__timeFilter(col)</td><td>Panel time-range filter for a timestamp column</td></tr>
            <tr><td>$__timeFrom()</td><td>Panel start timestamp</td></tr>
            <tr><td>$__timeTo()</td><td>Panel end timestamp</td></tr>
            <tr><td>$__timeGroup(col[, interval])</td><td>Timestamp bucket using the panel or supplied interval</td></tr>
            <tr><td>$__interval</td><td>Grafana’s calculated panel interval</td></tr>
          </tbody>
        </table>
        <code className={styles.codeBlock}>{DEFAULT_SQL}</code>
        <ul className={styles.text}>
          <li>Telemetry tables (points_double, points_int, points_string, points_struct, logs, channels) require a dataset_rid filter.</li>
          <li>Order by the time column for time series.</li>
          <li>Multi-value variables expand to {'\'a\',\'b\''}, so write channel IN ($channels) and single values as {'\'$channel\''}.</li>
        </ul>
      </div>
    </div>
  );
}
