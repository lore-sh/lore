import { describe, expect, test } from "bun:test";
import { getCommandHandler, runCli } from "../src/main";

async function expectUnknownCommand(command: string): Promise<void> {
  try {
    await runCli([command]);
    throw new Error(`Expected unknown command error for: ${command}`);
  } catch (error) {
    if (!(error instanceof Error)) {
      throw error;
    }
    expect(error.message).toBe(`Unknown command: ${command}`);
  }
}

describe("main command dispatch", () => {
  test("rejects prototype property names as commands", async () => {
    expect(getCommandHandler("toString")).toBeNull();
    expect(getCommandHandler("hasOwnProperty")).toBeNull();
    expect(getCommandHandler("__proto__")).toBeNull();

    await expectUnknownCommand("toString");
    await expectUnknownCommand("hasOwnProperty");
  });
});
