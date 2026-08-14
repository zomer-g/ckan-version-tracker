/**
 * cytoscape-fcose ships no types and has no @types package.
 *
 * Only the default export matters to us: it is the extension object handed to
 * `cytoscape.use()`. Declaring it as `unknown` rather than `any` keeps the
 * escape hatch from spreading — the call site casts once, deliberately.
 */
declare module "cytoscape-fcose" {
  const ext: unknown;
  export default ext;
}
