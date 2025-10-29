import {
  type Item,
  type ItemHighlight,
} from "jsr:@shougo/ddu-vim@~6.1.0/types";
import {
  BaseSource,
  type GatherArguments,
} from "jsr:@shougo/ddu-vim@~6.1.0/source";
import { treePath2Filename } from "jsr:@shougo/ddu-vim@~6.1.0/utils";
import { type ActionData } from "jsr:@shougo/ddu-kind-file@~0.9.0";

import type { Denops } from "jsr:@denops/core@7";
import * as fn from "jsr:@denops/std@7/function";

import { abortable } from "jsr:@std/async@1/abortable";
import { resolve } from "jsr:@std/path@1/resolve";
import { TextLineStream } from "jsr:@std/streams@1/text-line-stream";

const BUFFER_SIZES: readonly number[] = [
  100,
  1000,
  100000,
] as const;

const BUFFER_TIMEOUTS: readonly number[] = [
  500,
  1000,
  5000,
] as const;

type HighlightGroup = {
  path: string;
  lineNr: string;
  word: string;
};

type InputType =
  | "regex"
  | "migemo";

type Params = {
  args: string[];
  cmd: string;
  displayText: boolean;
  highlights: HighlightGroup;
  input: string;
  inputType: InputType;
  paths: string[];
};

type FileItem = Item<ActionData>;

declare namespace RipGrepJSON {
  type Text = {
    text?: string;
  };

  type SubMatch = {
    match: Text;
    start: number;
    end: number;
  };

  type Match = {
    type: "match";
    data: {
      path: Text;
      lines: Text;
      line_number: number;
      absolute_offset: number;
      submatches: SubMatch[];
    };
  };
}

type MatchData = {
  path: string;
  lineNr: number;
  col: number;
  text: string;
  submatches?: RipGrepJSON.SubMatch[];
};

const LOG_HEADER = "[ddu-source-rg]";

function logError(...data: unknown[]): void {
  console.error(LOG_HEADER, ...data);
}

function kensakuQuery(denops: Denops, text: string): Promise<string> {
  return denops.dispatch("kensaku", "query", text) as Promise<string>;
}

const utf8Encoder = new TextEncoder();
function utf8Length(str: string): number {
  return utf8Encoder.encode(str).length;
}

function childProcessAsDisposable<T extends Deno.ChildProcess>(
  childProcess: T,
): T & Disposable {
  return Object.defineProperty(childProcess, Symbol.dispose, {
    value: () => {
      try {
        childProcess.kill();
      } catch {
        // Prevent errors if already process closed.
      }
    },
  }) as T & Disposable;
}

function parseRawLine(line: string): MatchData | undefined {
  const [match, path, lineNr, col, text] = line
    .replace(/\r?\n$/, "")
    .match(/^([^:]+):(\d+):(\d+):(.*?)$/) ?? [];
  if (match == null) {
    return;
  }
  return {
    path,
    lineNr: parseInt(lineNr),
    col: parseInt(col),
    text,
  };
}

function parseJsonLine(line: string): MatchData | undefined {
  const json = line.trim();
  if (json === "") {
    return;
  }
  const obj = JSON.parse(json) as RipGrepJSON.Match;
  if (obj?.type !== "match") {
    return;
  }
  const { path, line_number, lines, submatches } = obj.data;
  return {
    path: path.text ?? "",
    lineNr: line_number,
    col: submatches[0].start + 1,
    text: lines.text?.replace(/\r?\n/, "") ?? "",
    submatches,
  };
}

class ErrorLogStream<T> extends WritableStream<T> {
  constructor() {
    super({
      write(msg) {
        logError(msg);
      },
    });
  }
}

class BufferStream<T> extends TransformStream<T, T[]> {
  constructor(options?: {
    bufferSizes: readonly number[];
    timeouts: readonly number[];
  }) {
    const DEFAULT_BUFFER_SIZE = 1;
    const DEFAULT_TIMEOUT = -1;
    const bufferSizeQueue = [...options?.bufferSizes ?? []];
    const timeoutQueue = [...options?.timeouts ?? []];

    let chunks: T[] = [];
    let controller: TransformStreamDefaultController<T[]>;
    let bufferSize: number;
    let timerId: number | undefined;

    const prepare = () => {
      bufferSize =
        (bufferSizeQueue.length > 1
          ? bufferSizeQueue.shift()
          : bufferSizeQueue[0]) ?? DEFAULT_BUFFER_SIZE;
      const timeout =
        (timeoutQueue.length > 1 ? timeoutQueue.shift() : timeoutQueue[0]) ??
          DEFAULT_TIMEOUT;
      if (timeout >= 0) {
        timerId = setInterval(flush, timeout);
      }
    };

    const flush = () => {
      clearInterval(timerId);
      if (chunks.length > 0) {
        controller?.enqueue(chunks);
        chunks = [];
      }
    };

    super({
      start(_controller) {
        controller = _controller;
        prepare();
      },
      transform(chunk) {
        chunks.push(chunk);
        if (chunks.length >= bufferSize) {
          flush();
          prepare();
        }
      },
      flush(controller) {
        flush();
        controller.terminate();
      },
      cancel() {
        clearInterval(timerId);
        return Promise.resolve();
      },
    });
  }
}

export class Source extends BaseSource<Params> {
  override kind = "file";

  override params(): Params {
    return {
      args: ["--column", "--no-heading", "--color", "never"],
      cmd: "rg",
      displayText: true,
      inputType: "regex",
      input: "",
      paths: [],
      highlights: {
        path: "Normal",
        lineNr: "Normal",
        word: "Search",
      },
    };
  }

  override gather(
    args: GatherArguments<Params>,
  ): ReadableStream<FileItem[]> {
    const aborter = new AbortController();
    return new ReadableStream({
      start: async (controller) => {
        const { signal } = aborter;
        const writable = new WritableStream({
          write(items) {
            controller.enqueue(items);
          },
        });
        try {
          await this.#processGatherStream(args, writable, signal);
        } catch (reason) {
          if (signal.aborted && reason === signal.reason) {
            // Abort by signal, so do nothing.
          } else {
            // Logging only internal errors.
            logError(reason);
          }
        } finally {
          try {
            controller.close();
          } catch {
            // Prevent errors if it is already closed.
          }
        }
      },
      cancel: (reason) => {
        aborter.abort(reason);
      },
    });
  }

  async #processGatherStream(
    args: GatherArguments<Params>,
    writable: WritableStream<FileItem[]>,
    signal: AbortSignal,
  ): Promise<void> {
    const { denops, sourceParams } = args;

    const [
      cmdPath,
      input,
      cwd,
    ] = await abortable(
      Promise.all([
        fn.exepath(denops, sourceParams.cmd),
        this.#getInput(args),
        this.#getCwd(args),
      ]),
      signal,
    );

    if (cmdPath === "") {
      logError(`Command not found: ${sourceParams.cmd}`);
      return;
    }
    if (input === "") {
      return;
    }

    const cmdArgs = [
      ...sourceParams.args,
      "--",
      input,
      ...sourceParams.paths,
    ];

    using proc = childProcessAsDisposable(
      new Deno.Command(
        cmdPath,
        {
          args: cmdArgs,
          stdout: "piped",
          stderr: "piped",
          stdin: "null",
          cwd,
          // Do not pass the `signal` to prevent a PermissionDenied error if
          // the child process is already closed.
        },
      ).spawn(),
    );

    const doneErrorStream = proc.stderr
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new TextLineStream())
      .pipeTo(new ErrorLogStream(), { signal });

    const doneOutputStream = proc.stdout
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new TextLineStream())
      .pipeThrough(this.#createItemStream(sourceParams, cwd))
      .pipeThrough(
        new BufferStream({
          bufferSizes: BUFFER_SIZES,
          timeouts: BUFFER_TIMEOUTS,
        }),
      )
      .pipeTo(writable, { signal });

    await Promise.all([doneErrorStream, doneOutputStream]);
  }

  async #getInput(args: GatherArguments<Params>): Promise<string> {
    const { denops, sourceOptions, sourceParams } = args;
    const { input } = sourceOptions.volatile ? args : sourceParams;
    switch (sourceParams.inputType) {
      case "migemo":
        return await kensakuQuery(denops, input);
      default: // "regex"
        return input;
    }
  }

  async #getCwd(args: GatherArguments<Params>): Promise<string> {
    const { denops, sourceOptions } = args;
    return sourceOptions.path.length > 0
      ? treePath2Filename(sourceOptions.path)
      : await fn.getcwd(denops) as string;
  }

  #getHighlightGroup(sourceParams: Params): HighlightGroup {
    return {
      path: "",
      lineNr: "",
      word: "",
      ...(sourceParams.highlights as Partial<HighlightGroup>),
    };
  }

  #createItemStream(
    sourceParams: Params,
    cwd: string,
  ): TransformStream<string, FileItem> {
    const { displayText } = sourceParams;
    const hlGroups = this.#getHighlightGroup(sourceParams);
    const parse = sourceParams.args.includes("--json")
      ? parseJsonLine
      : parseRawLine;

    return new TransformStream({
      transform(line, controller) {
        const data = parse(line);
        if (data == null) {
          return;
        }

        const { path, lineNr, col, text, submatches } = data;
        const absolutePath = path ? resolve(cwd, path) : "";
        const info = `${path}:${lineNr}:${col}`;
        const header = `${info}: `;
        const display = displayText ? header + text : info;

        const pathLen = utf8Length(path);
        const lineNrLen = utf8Length(`${lineNr}`);
        const headerLen = utf8Length(header);

        const highlights: ItemHighlight[] = [];

        if (hlGroups.path && pathLen > 0) {
          highlights.push({
            name: "path",
            hl_group: hlGroups.path,
            col: 1,
            width: pathLen,
          });
        }

        if (hlGroups.lineNr) {
          highlights.push(
            {
              name: "lineNr",
              hl_group: hlGroups.lineNr,
              col: 1 + pathLen + 1,
              width: lineNrLen,
            },
          );
        }

        if (hlGroups.word && displayText && submatches) {
          for (const { match, start, end } of submatches) {
            if (match.text) {
              highlights.push({
                name: "word",
                hl_group: hlGroups.word,
                col: 1 + headerLen + start,
                width: end - start,
              });
            }
          }
        }

        controller.enqueue({
          word: text,
          display,
          action: {
            path: absolutePath,
            lineNr,
            col,
            text,
          },
          highlights,
        });
      },
    });
  }
}
