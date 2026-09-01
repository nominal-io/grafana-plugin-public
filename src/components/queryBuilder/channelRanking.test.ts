import type { ChannelOption } from './queryBuilderTypes';
import { rankChannelOptions } from './channelRanking';

function opt(name: string): ChannelOption {
  return { label: name, value: name, dataType: 'numeric' };
}

function names(options: ChannelOption[]): string[] {
  return options.map((option) => option.value);
}

describe('rankChannelOptions', () => {
  it('ranks the exact match first when the server returns a delimiter variant first', () => {
    const query = 'vehicle.telemetry.engine_metrics.rpm_rx_count';
    const serverOrder = [
      opt('vehicle_telemetry_engine_metrics_rpm_rx_count'),
      opt(query),
      opt('vehicle.telemetry.engine_metrics.rpm_tx_count'),
    ];

    expect(names(rankChannelOptions(serverOrder, query))[0]).toBe(query);
  });

  it('ranks a full-name exact match above a segment exact match', () => {
    const result = rankChannelOptions([opt('a.temp'), opt('TEMP'), opt('temp')], 'temp');

    expect(names(result)).toEqual(['temp', 'TEMP', 'a.temp']);
  });

  it('ranks case-insensitive full equality above a bounded token match', () => {
    const result = rankChannelOptions([opt('engine.rpm.raw'), opt('ENGINE.RPM')], 'engine.rpm');

    expect(names(result)).toEqual(['ENGINE.RPM', 'engine.rpm.raw']);
  });

  it('ranks complete token matches equally across delimiter styles, above mid-word occurrences', () => {
    const result = rankChannelOptions(
      [opt('xengine_rpmx'), opt('motor.engine_rpm'), opt('axle_engine_rpm')],
      'engine_rpm'
    );

    expect(names(result)).toEqual(['axle_engine_rpm', 'motor.engine_rpm', 'xengine_rpmx']);
  });

  it('ranks a case-sensitive bounded token match above a case-insensitive one', () => {
    const result = rankChannelOptions([opt('a.TEMP.x'), opt('b.temp.x')], 'temp');

    expect(names(result)).toEqual(['b.temp.x', 'a.TEMP.x']);
  });

  it('orders tiers: bounded token, starts-with, word-starts-with, contains, subsequence', () => {
    const result = rankChannelOptions(
      [opt('r_p_m'), opt('xxrpmyy'), opt('speed_rpmzz'), opt('gearbox.speed_rpm_avg'), opt('rpmx.engine')],
      'rpm'
    );

    expect(names(result)).toEqual(['gearbox.speed_rpm_avg', 'rpmx.engine', 'speed_rpmzz', 'xxrpmyy', 'r_p_m']);
  });

  it('keeps non-matching rows after all matches, in server order', () => {
    const result = rankChannelOptions([opt('zeta.other'), opt('alpha.thing'), opt('rpm')], 'rpm');

    expect(names(result)).toEqual(['rpm', 'zeta.other', 'alpha.thing']);
  });

  it('breaks ties within a tier by natural numeric name order', () => {
    const result = rankChannelOptions([opt('sensor10.temp'), opt('sensor2.temp')], 'sensor');

    expect(names(result)).toEqual(['sensor2.temp', 'sensor10.temp']);
  });

  it('sorts alphabetically with natural numeric order when the query is blank', () => {
    const result = rankChannelOptions([opt('b.chan10'), opt('b.chan2'), opt('a.chan')], '   ');

    expect(names(result)).toEqual(['a.chan', 'b.chan2', 'b.chan10']);
  });

  it('returns a new permutation and does not mutate the input array', () => {
    const input = [opt('b'), opt('a')];
    const snapshot = [...input];

    const result = rankChannelOptions(input, 'zzz');

    expect(input).toEqual(snapshot);
    expect(result).not.toBe(input);
    expect(names(result)).toEqual(['b', 'a']);
  });
});
