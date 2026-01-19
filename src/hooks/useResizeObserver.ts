import { useEffect, useState, useRef, RefObject } from 'react';
import { useDebouncedCallback } from './useDebounce';

interface Size {
  width: number;
  height: number;
}

interface UseResizeObserverOptions {
  debounceMs?: number;
  initialDelay?: number;
}

/**
 * Hook to observe element size changes with optional debouncing.
 * Returns the current size of the observed element.
 */
export function useResizeObserver<T extends HTMLElement>(
  ref: RefObject<T>,
  options: UseResizeObserverOptions = {}
): Size {
  const { debounceMs = 150, initialDelay = 50 } = options;
  const [size, setSize] = useState<Size>({ width: 0, height: 0 });
  const observerRef = useRef<ResizeObserver | null>(null);

  const updateSize = useDebouncedCallback((entry: ResizeObserverEntry) => {
    const { width, height } = entry.contentRect;
    if (width > 0 && height > 0) {
      setSize({ width, height });
    }
  }, debounceMs);

  useEffect(() => {
    const element = ref.current;
    if (!element) return;

    // Initial size measurement after a short delay to ensure layout is complete
    const initialTimeout = setTimeout(() => {
      const rect = element.getBoundingClientRect();
      if (rect.width > 0 && rect.height > 0) {
        setSize({ width: rect.width, height: rect.height });
      }
    }, initialDelay);

    // Set up resize observer
    observerRef.current = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        updateSize(entry);
      }
    });

    observerRef.current.observe(element);

    return () => {
      clearTimeout(initialTimeout);
      if (observerRef.current) {
        observerRef.current.disconnect();
      }
    };
  }, [ref, updateSize, initialDelay]);

  return size;
}
