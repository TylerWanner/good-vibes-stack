type PluginContext = {
  config?: {
    baseUrl?: string;
    timeoutMs?: number;
    apiKey?: string;
  };
  registerTool: (tool: {
    name: string;
    description: string;
    parameters?: object;
    input_schema?: object;
    inputSchema?: object;
    execute?: (_id: string, args: Record<string, unknown>) => Promise<unknown>;
    handler?: (args: Record<string, unknown>) => Promise<unknown>;
  }) => void;
};

function makeBaseUrl(raw?: string): string {
  const value = (raw || "http://host.docker.internal:8001").trim();
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function getApiKey(configKey?: string): string | undefined {
  return configKey || process.env.API_SECRET_KEY || undefined;
}

async function requestJson(
  url: string,
  options: RequestInit,
  timeoutMs: number,
): Promise<unknown> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${text}`);
    }
    try {
      return JSON.parse(text);
    } catch {
      return { raw: text };
    }
  } finally {
    clearTimeout(timer);
  }
}

function asToolResult(payload: unknown): { content: Array<{ type: "text"; text: string }> } {
  return {
    content: [{ type: "text", text: JSON.stringify(payload, null, 2) }],
  };
}

export default function secondBrainTools(context: PluginContext): void {
  const baseUrl = makeBaseUrl(context.config?.baseUrl);
  const timeoutMs = context.config?.timeoutMs ?? 45000;
  const apiKey = getApiKey(context.config?.apiKey);
  
  const authHeaders: Record<string, string> = apiKey ? { "X-API-Key": apiKey } : {};

  context.registerTool({
    name: "second_brain_save_content",
    description:
      "Save and analyze a URL using the second-brain workflow. Use this whenever a user sends a URL, asks to save/bookmark/ingest content, or asks to add content to second brain.",
    parameters: {
      type: "object",
      properties: {
        url: { type: "string", description: "URL to ingest" },
        force: { type: "boolean", description: "Force re-ingest even if URL already exists", default: false },
        notify: {
          type: "object",
          description: "Optional notification routing envelope for async completion messages",
          properties: {
            context: { type: "string" },
            channel: { type: "string" },
            account_id: { type: "string" },
            chat_id: { type: "string" },
            reply_to_message_id: { type: "string" }
          }
        }
      },
      required: ["url"]
    },
    input_schema: {
      type: "object",
      properties: {
        url: { type: "string", description: "URL to ingest" },
        force: { type: "boolean", description: "Force re-ingest even if URL already exists", default: false },
        notify: {
          type: "object",
          description: "Optional notification routing envelope for async completion messages",
          properties: {
            context: { type: "string" },
            channel: { type: "string" },
            account_id: { type: "string" },
            chat_id: { type: "string" },
            reply_to_message_id: { type: "string" }
          }
        }
      },
      required: ["url"]
    },
    inputSchema: {
      type: "object",
      properties: {
        url: { type: "string", description: "URL to ingest" },
        force: { type: "boolean", description: "Force re-ingest even if URL already exists", default: false },
        notify: {
          type: "object",
          description: "Optional notification routing envelope for async completion messages",
          properties: {
            context: { type: "string" },
            channel: { type: "string" },
            account_id: { type: "string" },
            chat_id: { type: "string" },
            reply_to_message_id: { type: "string" }
          }
        }
      },
      required: ["url"]
    },
    execute: async (_id, args) => {
      const url = String(args.url || "");
      const force = args.force === true;
      const notifyArg = (args.notify && typeof args.notify === "object") ? args.notify as Record<string, unknown> : {};
      const notify = {
        context: typeof notifyArg.context === "string" ? notifyArg.context : (process.env.AGENT_NAME || undefined),
        channel: typeof notifyArg.channel === "string" ? notifyArg.channel : undefined,
        account_id: typeof notifyArg.account_id === "string" ? notifyArg.account_id : undefined,
        chat_id: typeof notifyArg.chat_id === "string" ? notifyArg.chat_id : undefined,
        reply_to_message_id: typeof notifyArg.reply_to_message_id === "string" ? notifyArg.reply_to_message_id : undefined,
      };
      const endpoint = new URL("/ingest", baseUrl);

      const payload = await requestJson(
        endpoint.toString(),
        {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders },
          body: JSON.stringify({ url, force, notify }),
        },
        timeoutMs,
      );
      return asToolResult(payload);
    }
  });

  context.registerTool({
    name: "second_brain_weekly_digest",
    description:
      "Generate a weekly digest from processed second-brain articles. Use this when users ask for a digest/summary over recent saved content.",
    parameters: {
      type: "object",
      properties: {
        days: { type: "integer", minimum: 1, maximum: 30, default: 7 }
      }
    },
    input_schema: {
      type: "object",
      properties: {
        days: { type: "integer", minimum: 1, maximum: 30, default: 7 }
      }
    },
    inputSchema: {
      type: "object",
      properties: {
        days: { type: "integer", minimum: 1, maximum: 30, default: 7 }
      }
    },
    execute: async (_id, args) => {
      const days = Number.isFinite(Number(args.days)) ? Number(args.days) : 7;
      const endpoint = new URL("/digest", baseUrl);
      endpoint.searchParams.set("days", String(days));

      const payload = await requestJson(
        endpoint.toString(),
        { method: "POST", headers: authHeaders },
        timeoutMs,
      );
      return asToolResult(payload);
    }
  });

  context.registerTool({
    name: "second_brain_search",
    description:
      "Search ingested second-brain knowledge. Use this for questions about saved content, recent items, tags, sources, or lookups in second brain.",
    parameters: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query" },
        limit: { type: "integer", minimum: 1, maximum: 50, default: 10 }
      },
      required: ["query"]
    },
    input_schema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query" },
        limit: { type: "integer", minimum: 1, maximum: 50, default: 10 }
      },
      required: ["query"]
    },
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query" },
        limit: { type: "integer", minimum: 1, maximum: 50, default: 10 }
      },
      required: ["query"]
    },
    execute: async (_id, args) => {
      const query = String(args.query || "");
      const limit = Number.isFinite(Number(args.limit)) ? Number(args.limit) : 10;
      const endpoint = new URL("/ingest", baseUrl);
      endpoint.searchParams.set("q", query);
      endpoint.searchParams.set("limit", String(limit));

      const payload = await requestJson(
        endpoint.toString(),
        { method: "GET", headers: authHeaders },
        timeoutMs,
      );
      return asToolResult(payload);
    }
  });
  context.registerTool({
    name: "second_brain_reingest",
    description:
      "Search for articles in the second brain by query and re-ingest them with the latest model and fetchers. Use when summaries are thin or content needs refreshing.",
    parameters: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query to find articles to reingest" },
        limit: { type: "integer", minimum: 1, maximum: 20, default: 5 }
      },
      required: ["query"]
    },
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query to find articles to reingest" },
        limit: { type: "integer", minimum: 1, maximum: 20, default: 5 }
      },
      required: ["query"]
    },
    execute: async (_id, args) => {
      const query = String(args.query || "");
      const limit = Number.isFinite(Number(args.limit)) ? Number(args.limit) : 5;
      const endpoint = new URL("/ingest/reprocess", baseUrl);
      endpoint.searchParams.set("query", query);
      endpoint.searchParams.set("limit", String(limit));

      const payload = await requestJson(
        endpoint.toString(),
        { method: "POST", headers: authHeaders },
        timeoutMs,
      );
      return asToolResult(payload);
    }
  });
}
