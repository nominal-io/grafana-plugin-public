import { act, renderHook } from '@testing-library/react';
import { useCopyToClipboard } from './useCopyToClipboard';

describe('useCopyToClipboard', () => {
  let writeText: jest.Mock;
  let originalExecCommand: typeof document.execCommand;

  beforeEach(() => {
    writeText = jest.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    originalExecCommand = document.execCommand;
    jest.useFakeTimers();
  });

  afterEach(() => {
    document.execCommand = originalExecCommand;
    jest.useRealTimers();
  });

  it('writes to the clipboard and shows the copied message', async () => {
    const { result } = renderHook(() => useCopyToClipboard());
    expect(result.current.showCopiedMessage).toBe(false);

    await act(async () => {
      await result.current.copyToClipboard('ri.asset.1');
    });

    expect(writeText).toHaveBeenCalledWith('ri.asset.1');
    expect(result.current.showCopiedMessage).toBe(true);
  });

  it('hides the copied message after 2 seconds', async () => {
    const { result } = renderHook(() => useCopyToClipboard());
    await act(async () => {
      await result.current.copyToClipboard('ri.asset.1');
    });
    expect(result.current.showCopiedMessage).toBe(true);

    act(() => {
      jest.advanceTimersByTime(2000);
    });
    expect(result.current.showCopiedMessage).toBe(false);
  });

  it('falls back to execCommand when the clipboard API rejects', async () => {
    writeText.mockRejectedValue(new Error('no clipboard'));
    const execCommand = jest.fn();
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    document.execCommand = execCommand as unknown as typeof document.execCommand;

    const { result } = renderHook(() => useCopyToClipboard());
    await act(async () => {
      await result.current.copyToClipboard('ri.asset.1');
    });

    expect(execCommand).toHaveBeenCalledWith('copy');
    expect(result.current.showCopiedMessage).toBe(true);
  });

  it('does not schedule a hide timer after unmount when the clipboard write resolves', async () => {
    let resolveWrite: () => void;
    writeText.mockReturnValue(
      new Promise<void>((resolve) => {
        resolveWrite = resolve;
      })
    );

    const { result, unmount } = renderHook(() => useCopyToClipboard());
    let copyPromise: Promise<void>;
    act(() => {
      copyPromise = result.current.copyToClipboard('ri.asset.1');
    });

    unmount();

    await act(async () => {
      resolveWrite();
      await copyPromise;
    });

    expect(jest.getTimerCount()).toBe(0);
  });

  it('keeps one active hide timer when overlapping clipboard writes resolve out of order', async () => {
    let resolveFirstWrite: () => void;
    let resolveSecondWrite: () => void;
    writeText
      .mockReturnValueOnce(
        new Promise<void>((resolve) => {
          resolveFirstWrite = resolve;
        })
      )
      .mockReturnValueOnce(
        new Promise<void>((resolve) => {
          resolveSecondWrite = resolve;
        })
      );

    const { result } = renderHook(() => useCopyToClipboard());
    let firstCopyPromise: Promise<void>;
    let secondCopyPromise: Promise<void>;
    act(() => {
      firstCopyPromise = result.current.copyToClipboard('ri.asset.1');
      secondCopyPromise = result.current.copyToClipboard('ri.asset.2');
    });

    await act(async () => {
      resolveSecondWrite();
      await secondCopyPromise;
    });
    expect(result.current.showCopiedMessage).toBe(true);
    expect(jest.getTimerCount()).toBe(1);

    await act(async () => {
      resolveFirstWrite();
      await firstCopyPromise;
    });
    expect(result.current.showCopiedMessage).toBe(true);
    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(2000);
    });
    expect(result.current.showCopiedMessage).toBe(false);
    expect(jest.getTimerCount()).toBe(0);
  });
});
