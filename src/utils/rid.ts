// Same grammar the backend parses RIDs with.
const RID_PATTERN = /^ri\.[a-z][a-z0-9-]*\.(?:[a-z0-9][a-z0-9-]*)?\.([a-z][a-z0-9-]*)\.[\w.-]+$/;

/** The type segment of `ri.<service>.<instance>.<type>.<locator>`, or undefined for other text. */
export function ridType(value: string): string | undefined {
  return RID_PATTERN.exec(value.trim())?.[1];
}

/**
 * How many values a variable interpolated to. A RID never contains a comma or a pipe, so either
 * one separates values, in the default {a,b} format or in csv, pipe, json, and similar.
 */
export const ridValueCount = (value: string): number => value.split(/[,|]/).length;
