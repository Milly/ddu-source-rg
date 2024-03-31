import { type Denops, fn } from "https://deno.land/x/ddu_vim@v3.10.3/deps.ts";
import {
  BaseSource,
  type Item,
  type SourceOptions,
} from "https://deno.land/x/ddu_vim@v3.10.3/types.ts";
import type { GatherArguments } from "https://deno.land/x/ddu_vim@v3.10.3/base/source.ts";
import { treePath2Filename } from "https://deno.land/x/ddu_vim@v3.10.3/utils.ts";
import type { ActionData } from "https://deno.land/x/ddu_kind_file@v0.7.1/file.ts";
import { resolve } from "https://deno.land/std@0.221.0/path/mod.ts";
import { TextLineStream } from "https://deno.land/std@0.221.0/streams/mod.ts";
import { ItemHighlight } from "https://deno.land/x/ddu_vim@v3.4.3/types.ts";

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
const log = {
  error: console.error.bind(console, LOG_HEADER),
};

function kensakuQuery(denops: Denops, text: string): Promise<string> {
  return denops.dispatch("kensaku", "query", text) as Promise<string>;
}

const utf8Encoder = new TextEncoder();
function utf8Length(str: string): number {
  return utf8Encoder.encode(str).length;
}

type DisposableChildProcess = Deno.ChildProcess & {
  [Symbol.dispose](): void;
};

function spawnDisposableChildProcess(
  command: Deno.Command,
): DisposableChildProcess {
  const proc = command.spawn() as DisposableChildProcess;
  proc[Symbol.dispose] = () => {
    try {
      proc.kill();
    } catch {
      // Prevent errors if already process closed.
    }
  };
  return proc;
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

class ErrorLogStream extends WritableStream {
  constructor() {
    super({
      write(stderr) {
        log.error(stderr);
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
    const { readable, writable } = new TransformStream<FileItem[]>();
    this.#startGatherStream(args, writable);
    return readable;
  }

  async #startGatherStream(
    args: GatherArguments<Params>,
    outputStream: WritableStream<FileItem[]>,
  ): Promise<void> {
    const { denops, sourceOptions, sourceParams } = args;

    try {
      const cmdPath = await fn.exepath(denops, sourceParams.cmd);
      if (cmdPath === "") {
        log.error(`Command not found: ${sourceParams.cmd}`);
        return;
      }

      const input = await this.#getInput(args);
      if (input === "") {
        return;
      }

      const cwd = await this.#getCwd(denops, sourceOptions);

      const cmdArgs = [
        ...sourceParams.args,
        "--",
        input,
        ...sourceParams.paths,
      ];

      using proc = spawnDisposableChildProcess(
        new Deno.Command(
          cmdPath,
          {
            args: cmdArgs,
            stdout: "piped",
            stderr: "piped",
            stdin: "null",
            cwd,
          },
        ),
      );

      const doneErrorStream = proc.stderr
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(new TextLineStream())
        .pipeTo(new ErrorLogStream());

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
        .pipeTo(outputStream, {
          // Do not output internal errors to the stream.
          preventAbort: true,
        });

      await Promise.race([doneErrorStream, doneOutputStream]);
    } catch (reason) {
      // Logging only errors.
      // NOTE: If canceled by ddu, `reason` is a string. (e.g. "quit")
      if (typeof reason !== "string") {
        log.error(reason);
      }
    } finally {
      try {
        await outputStream.close();
      } catch {
        // Prevent errors if it is already closed.
      }
    }
  }

  async #getInput(
    args: GatherArguments<Params>,
  ): Promise<string> {
    const { denops, sourceOptions, sourceParams } = args;
    const { input } = sourceOptions.volatile ? args : sourceParams;
    switch (sourceParams.inputType) {
      case "migemo":
        return await kensakuQuery(denops, input);
      default: // "regex"
        return input;
    }
  }

  async #getCwd(denops: Denops, sourceOptions: SourceOptions): Promise<string> {
    return sourceOptions.path.length > 0
      ? treePath2Filename(sourceOptions.path)
      : await fn.getcwd(denops) as string;
  }

  #getHighlightGroup(
    sourceParams: Params,
  ): HighlightGroup {
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
          for (const { match, start } of submatches) {
            if (match.text) {
              highlights.push({
                name: "word",
                hl_group: hlGroups.word,
                col: 1 + headerLen + utf8Length(text.slice(0, start)),
                width: utf8Length(match.text!),
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
