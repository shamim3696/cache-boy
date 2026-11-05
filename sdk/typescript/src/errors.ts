/**
 * Every error the SDK produces is a CacheBoyError.
 *
 * `code` is the first word of a server error (WRONGTYPE, NOPERM, MOVED, ERR...)
 * or one of the client-side codes below.
 */
export class CacheBoyError extends Error {
  /** First word of the server error, or a client-side code. */
  readonly code: string;
  /** The command that failed, when known (e.g. "SET"). */
  command?: string;

  constructor(message: string, code?: string, options?: { cause?: unknown; command?: string }) {
    super(message, options?.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = 'CacheBoyError';
    this.code = code ?? codeOf(message);
    if (options?.command !== undefined) this.command = options.command;
  }

  /** Builds an error from a server error line such as "WRONGTYPE Operation against a key...". */
  static fromServer(text: string, command?: string): CacheBoyError {
    return new CacheBoyError(text, codeOf(text), command !== undefined ? { command } : undefined);
  }
}

/** Client-side error codes (never sent by the server). */
export const ErrorCodes = {
  /** The command didn't get a reply within its timeout. */
  TIMEOUT: 'TIMEOUT',
  /** The connection dropped after the command was written, and it isn't safe to retry. */
  CONNECTION_LOST: 'CONNECTION_LOST',
  /** The client was closed (quit/disconnect) or gave up reconnecting. */
  CONNECTION_CLOSED: 'CONNECTION_CLOSED',
  /** The command isn't available on this transport (e.g. SUBSCRIBE over HTTP). */
  UNSUPPORTED: 'UNSUPPORTED',
  /** The server sent bytes that aren't valid RESP. */
  PROTOCOL: 'PROTOCOL',
  /** An HTTP request failed at the transport level. */
  HTTP: 'HTTP',
  /** withLock() couldn't acquire the lock. */
  LOCKED: 'LOCKED',
  /** A reply didn't have the shape the typed API expected. */
  BAD_REPLY: 'BAD_REPLY',
} as const;

function codeOf(message: string): string {
  const sp = message.indexOf(' ');
  const word = sp === -1 ? message : message.slice(0, sp);
  // Redis convention: an all-caps first word is the error code.
  return /^[A-Z][A-Z0-9_]*$/.test(word) ? word : 'ERR';
}
