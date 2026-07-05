/**
 * Minimal class error boundary. React has no hook equivalent — a render-phase
 * throw in a child (e.g. Leaflet's `addData` rejecting a feature with an empty
 * `geometry: {}`) would otherwise unmount the WHOLE app and blank the page.
 * Wrap risky subtrees (the map) so a failure degrades to `fallback` instead.
 */
import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
  /** Optional label for the console line, to tell instances apart. */
  label?: string;
}

interface State {
  hasError: boolean;
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false };

  static getDerivedStateFromError(): State {
    return { hasError: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Keep the diagnostic — but never re-throw; the point is graceful degrade.
    console.error(`[ErrorBoundary${this.props.label ? " " + this.props.label : ""}]`, error, info);
  }

  render(): ReactNode {
    if (this.state.hasError) return this.props.fallback ?? null;
    return this.props.children;
  }
}
