import { useCallback, useEffect, useRef, useState } from 'react';

export interface CopyToClipboardModel {
  showCopiedMessage: boolean;
  copyToClipboard: (text: string) => Promise<void>;
}

/** Owns the "copied" tooltip lifecycle. No domain coupling. */
export function useCopyToClipboard(): CopyToClipboardModel {
  const [showCopiedMessage, setShowCopiedMessage] = useState(false);
  // Ref for the "copied" tooltip hide-timer so it can be cleared on unmount.
  const copiedTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const isMountedRef = useRef(true);
  const copyRequestIdRef = useRef(0);

  const showCopiedMessageAndScheduleHide = useCallback((copyRequestId: number) => {
    if (!isMountedRef.current || copyRequestId !== copyRequestIdRef.current) {
      return;
    }

    clearTimeout(copiedTimerRef.current);
    setShowCopiedMessage(true);
    copiedTimerRef.current = setTimeout(() => {
      if (isMountedRef.current && copyRequestId === copyRequestIdRef.current) {
        setShowCopiedMessage(false);
      }
    }, 2000);
  }, []);

  const copyToClipboard = useCallback(async (text: string) => {
    const copyRequestId = copyRequestIdRef.current + 1;
    copyRequestIdRef.current = copyRequestId;

    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // Fallback for browsers that don't support clipboard API
      const textArea = document.createElement('textarea');
      textArea.value = text;
      document.body.appendChild(textArea);
      try {
        textArea.select();
        // eslint-disable-next-line @typescript-eslint/no-deprecated
        document.execCommand('copy');
      } finally {
        document.body.removeChild(textArea);
      }
    }

    showCopiedMessageAndScheduleHide(copyRequestId);
  }, [showCopiedMessageAndScheduleHide]);

  useEffect(() => {
    return () => {
      isMountedRef.current = false;
      clearTimeout(copiedTimerRef.current);
    };
  }, []);

  return { showCopiedMessage, copyToClipboard };
}
