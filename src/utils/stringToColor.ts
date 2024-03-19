export function stringToColor(string: string) {
  let hash = 0;

  for (let i = 0; i < string.length; i++) {
    // 9 << 3 => 9 * (2 ** 3) = 36
    hash = string.charCodeAt(i) + ((hash << 5) - hash);
  }

  let color = "#";

  for (let i = 0; i < 3; i++) {
    const value = (hash >> (i * 8)) & 0xff;
    color += `00${value.toString(16)}`.slice(-2);
  }

  return color;
}
