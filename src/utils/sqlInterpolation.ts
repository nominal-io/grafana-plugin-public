function quote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

export function sqlInterpolateVariable(
  value: string | string[] | number,
  variable: { multi?: boolean; includeAll?: boolean }
): string {
  if (typeof value === 'number') {
    return String(value);
  }

  if (typeof value === 'string') {
    return variable.multi || variable.includeAll ? quote(value) : value;
  }

  return value.map(quote).join(',');
}

export function isSqlVariableQuery(query: string): boolean {
  return /^\s*(select|with)\b/i.test(query);
}
