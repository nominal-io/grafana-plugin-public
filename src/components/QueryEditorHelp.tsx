import React from 'react';
import { css } from '@emotion/css';
import { Button, Stack, useStyles2 } from '@grafana/ui';
import { GrafanaTheme2, QueryEditorHelpProps } from '@grafana/data';

import { NominalQuery, DEFAULT_AGGREGATIONS } from '../types';

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
});

const timeSeriesExample: NominalQuery = {
  refId: 'A',
  assetInputMethod: 'direct',
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
    </div>
  );
}
