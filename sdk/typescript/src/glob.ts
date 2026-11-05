/** Redis-style glob matching: `*`, `?`, `[abc]`, `[^a]`, `[a-z]`, `\x`. */
export function globMatch(pattern: string, s: string): boolean {
  return match(pattern, 0, s, 0);
}

function match(p: string, pi: number, s: string, si: number): boolean {
  while (pi < p.length) {
    const c = p[pi]!;
    if (c === '*') {
      while (p[pi] === '*') pi++;
      if (pi === p.length) return true;
      for (let k = si; k <= s.length; k++) if (match(p, pi, s, k)) return true;
      return false;
    }
    if (si >= s.length) return false;
    if (c === '?') {
      pi++;
      si++;
      continue;
    }
    if (c === '[') {
      let j = pi + 1;
      let neg = false;
      if (p[j] === '^') {
        neg = true;
        j++;
      }
      let ok = false;
      const ch = s[si]!;
      while (j < p.length && p[j] !== ']') {
        if (p[j] === '\\' && j + 1 < p.length) {
          j++;
          if (p[j] === ch) ok = true;
          j++;
        } else if (p[j + 1] === '-' && j + 2 < p.length && p[j + 2] !== ']') {
          let lo = p[j]!;
          let hi = p[j + 2]!;
          if (lo > hi) [lo, hi] = [hi, lo];
          if (ch >= lo && ch <= hi) ok = true;
          j += 3;
        } else {
          if (p[j] === ch) ok = true;
          j++;
        }
      }
      if (ok === neg) return false;
      pi = j + 1;
      si++;
      continue;
    }
    if (c === '\\' && pi + 1 < p.length) {
      if (p[pi + 1] !== s[si]) return false;
      pi += 2;
      si++;
      continue;
    }
    if (c !== s[si]) return false;
    pi++;
    si++;
  }
  return si === s.length;
}
