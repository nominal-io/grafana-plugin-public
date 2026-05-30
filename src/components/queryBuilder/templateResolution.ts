import type { NominalQuery } from '../../types';

export interface TemplateValueResolution {
  raw: string;
  resolved: string;
  hasTemplate: boolean;
  isResolved: boolean;
}

export interface QueryTemplateResolution {
  assetRid: TemplateValueResolution;
  dataScopeName: TemplateValueResolution;
  channel: TemplateValueResolution;
}

export type TemplateValueReplacer = (value: string) => string;

export function resolveTemplateValue(rawValue: string | undefined, replace: TemplateValueReplacer): TemplateValueResolution {
  const raw = rawValue || '';
  const hasTemplate = raw.includes('$');
  const resolved = raw ? replace(raw) : '';

  return {
    raw,
    resolved,
    hasTemplate,
    isResolved: !resolved.includes('$'),
  };
}

export function resolveQueryTemplateValues({
  query,
  replace,
}: {
  query: NominalQuery | undefined;
  replace: TemplateValueReplacer;
}): QueryTemplateResolution {
  return {
    assetRid: resolveTemplateValue(query?.assetRid, replace),
    dataScopeName: resolveTemplateValue(query?.dataScopeName, replace),
    channel: resolveTemplateValue(query?.channel, replace),
  };
}

export function templateDisplayLabel(value: TemplateValueResolution): string {
  if (value.hasTemplate && value.isResolved && value.resolved && value.resolved !== value.raw) {
    return `${value.raw} \u2192 ${value.resolved}`;
  }
  return value.raw;
}
