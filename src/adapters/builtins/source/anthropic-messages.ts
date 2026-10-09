import type { SourceAdapter } from '../../../types';
import { ok } from '../../../types';
import { asBoolean, isObject } from '../../../utils';
import { buildAnthropicHeaders } from '../common';
import { anthropicToolId } from '../anthropic-tool-id';
import { formatAnthropicMessagesResponse } from './formatters';
import { parseAnthropicMessagesRequest } from './parsers';

export const anthropicMessagesSourceAdapter: SourceAdapter = {
  key: 'anthropic_messages',
  provider: 'anthropic',
  toStandardRequest(input) {
    return parseAnthropicMessagesRequest(input.body);
  },
  fromStandardResponse(input) {
    return formatAnthropicMessagesResponse(input.response);
  },
  isStreamingRequest(input) {
    return asBoolean(input.body.stream) === true;
  },
  buildPassthroughRequest(input) {
    const headersResult = buildAnthropicHeaders(input.request.headers, input.config);
    if (!headersResult.ok) {
      return headersResult;
    }

    return ok({
      url: `${input.config.anthropicBaseUrl}/v1/messages`,
      headers: headersResult.value,
      body: {
        ...input.body,
        messages: Array.isArray(input.body.messages) ? input.body.messages.map((message: unknown) => !isObject(message) || Array.isArray(message) ? message : ({
          ...message,
          content: Array.isArray(message.content) ? message.content.map((block: unknown) => {
            if (!isObject(block) || Array.isArray(block)) return block;
            if (block.type === 'tool_use' || block.type === 'server_tool_use') {
              return { ...block, id: anthropicToolId(typeof block.id === 'string' ? block.id : undefined) };
            }
            if (typeof block.tool_use_id === 'string') {
              return { ...block, tool_use_id: anthropicToolId(block.tool_use_id) };
            }
            return block;
          }) : message.content
        })) : input.body.messages
      }
    });
  }
};
