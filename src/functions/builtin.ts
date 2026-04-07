import { FunctionRegistry } from '../core/registry/function-registry.js';

export function registerBuiltinFunctions(registry: FunctionRegistry): void {
  // Math
  registry.register({
    name: 'ABS',
    inferType: () => ({ kind: 'number' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'ABS takes 1 argument' },
            ],
          },
    execute: args => Math.abs(args[0] as number),
  });

  registry.register({
    name: 'FLOOR',
    inferType: () => ({ kind: 'number' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'FLOOR takes 1 argument' },
            ],
          },
    execute: args => Math.floor(args[0] as number),
  });

  registry.register({
    name: 'CEIL',
    inferType: () => ({ kind: 'number' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'CEIL takes 1 argument' },
            ],
          },
    execute: args => Math.ceil(args[0] as number),
  });

  registry.register({
    name: 'ROUND',
    inferType: () => ({ kind: 'number' }),
    validate: args =>
      args.length >= 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'ROUND takes 1-2 arguments' },
            ],
          },
    execute: args => {
      const decimals = (args[1] as number) ?? 0;
      return (
        Math.round((args[0] as number) * Math.pow(10, decimals)) /
        Math.pow(10, decimals)
      );
    },
  });

  registry.register({
    name: 'SQRT',
    inferType: () => ({ kind: 'number' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'SQRT takes 1 argument' },
            ],
          },
    execute: args => Math.sqrt(args[0] as number),
  });

  // String
  registry.register({
    name: 'UPPER',
    inferType: () => ({ kind: 'string' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'UPPER takes 1 argument' },
            ],
          },
    execute: args => String(args[0]).toUpperCase(),
  });

  registry.register({
    name: 'LOWER',
    inferType: () => ({ kind: 'string' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'LOWER takes 1 argument' },
            ],
          },
    execute: args => String(args[0]).toLowerCase(),
  });

  registry.register({
    name: 'TRIM',
    inferType: () => ({ kind: 'string' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'TRIM takes 1 argument' },
            ],
          },
    execute: args => String(args[0]).trim(),
  });

  registry.register({
    name: 'LENGTH',
    inferType: () => ({ kind: 'number' }),
    validate: args =>
      args.length === 1
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'LENGTH takes 1 argument' },
            ],
          },
    execute: args => String(args[0]).length,
  });

  registry.register({
    name: 'CONCAT',
    inferType: () => ({ kind: 'string' }),
    validate: args =>
      args.length >= 2
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'CONCAT takes 2+ arguments' },
            ],
          },
    execute: args => args.map(String).join(''),
  });

  registry.register({
    name: 'COALESCE',
    inferType: args => args[0] ?? { kind: 'any' },
    validate: args =>
      args.length >= 2
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'COALESCE takes 2+ arguments' },
            ],
          },
    execute: args =>
      args.find(a => a !== null && a !== undefined) ?? null,
  });

  // Date
  registry.register({
    name: 'NOW',
    inferType: () => ({ kind: 'number' }),
    validate: () => ({ ok: true }),
    execute: () => Date.now(),
  });

  registry.register({
    name: 'DATE_TRUNC',
    inferType: () => ({ kind: 'string' }),
    validate: args =>
      args.length === 2
        ? { ok: true }
        : {
            ok: false,
            errors: [
              { code: 'WRONG_ARGS', message: 'DATE_TRUNC takes 2 arguments' },
            ],
          },
    execute: args => {
      const unit = args[0] as string;
      const date = new Date(args[1] as number);
      if (unit === 'day') return date.toISOString().split('T')[0];
      if (unit === 'month') return date.toISOString().slice(0, 7);
      if (unit === 'year') return String(date.getFullYear());
      return date.toISOString();
    },
  });
}
