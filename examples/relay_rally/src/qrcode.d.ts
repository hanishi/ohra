// The global that qrcode-generator defines (loaded from the CDN by wall.html), as far as the
// wall uses it.
declare function qrcode(
  typeNumber: number,
  level: "L" | "M" | "Q" | "H",
): {
  addData(s: string): void;
  make(): void;
  getModuleCount(): number;
  isDark(r: number, c: number): boolean;
};
