# Development Guidelines

## Development Process (STRICT TDD - MANDATORY!)

**CRITICAL: You MUST follow this exact workflow. No shortcuts.**

### Step 1: Create STUB functions with documentation
- Write function signature with precise, concise doc comment
- **Document what errors can be returned and under which circumstances**
- **Reference specific error types (sentinel errors like ErrNotFound, io.EOF, context.Canceled)**
- Body should be minimal: `panic("not implemented")` or `return errors.New("not implemented")`
- This defines the CONTRACT before any implementation
- Example:
  ```go
  // Delete removes a sharded file and all its chunks from storage.
  //
  // Returns an error if:
  //   - The manifest doesn't exist (wraps gcerrors.NotFound)
  //   - A chunk cannot be deleted (permission, network error)
  //   - The context is cancelled (context.Canceled or context.DeadlineExceeded)
  func Delete(ctx context.Context, bucket *blob.Bucket, dest string) error {
      panic("not implemented")
  }
  ```

### Step 2: Write tests FIRST
- Create tests that verify the documented behavior
- Use table-driven tests for multiple scenarios
- **Run tests immediately - they MUST FAIL** (panic or error)
- If tests pass without implementation, tests are wrong

### Step 3: Implement the function
- Replace panic/error stub with real implementation
- **Run tests after implementation**
- Watch tests change from failing to passing

### Step 4: Verify and refine
- If tests fail, think HARD: is expectation wrong or implementation bad?
- Do not blindly change code - understand the failure first
- Stop implementing when tests pass - do not add extra code

### Step 5: Keep it simple
- Prefer simpler implementations over complex logic
- Don't over-engineer
- If you haven't run `go test` in the last 2-3 tool calls, you're doing it wrong

## Tool Preferences

- **Use MCP tools**: Prefer `gopls` over grep searches, `delve` over printf debugging
- `mcp__gopls__*` - For code navigation, references, documentation
- `mcp__delve__*` - For debugging, stepping through code

## Anti-Cleverness Guidelines

### 1. Read First, Code Second
- Before implementing, understand what already exists
- When told to reference external code, read it completely first
- Ask: "What does existing implementation actually do?" not "What would be a good way?"

### 2. Explicit Constraints Are Hard Constraints
- Treat "DO NOT use X" or "NO Y" as absolute constraints
- If you catch yourself thinking "but what if I...", stop and re-read constraints

### 3. Resist the "Surely They Mean..." Trap
- Don't interpret requirements beyond what's written
- When in doubt, follow the literal instruction

### 4. Simplicity Bias
- If an implementation looks "too simple", it probably IS simple
- Complex solutions are often wrong when simple approaches exist

### 5. Stop and Check Rule
- Before implementing any algorithm, ask: "Did they explicitly tell me NOT to do this?"

## Go Code Guidelines

### Style
- Use the most idiomatic Go code
- Use meaningful variable and function names
- Write clear, concise comments for complex logic; avoid redundant comments
- Avoid magic numbers/strings; use constants
- Keep functions small and focused on a single task

### Tools
- Always use `go vet` to check code
- Always use `go fmt` to format code when done
- Use `gopls` MCP tools to lookup references/documentation (avoid grep)
- Use `go doc` to review documentation
- Use `go test` to run tests before committing

### Dependencies
- Prefer stdlib packages over third-party libraries unless necessary
- Use existing packages in the codebase where applicable
- Use `go mod` for dependency management; keep dependencies up to date

### Error Handling
- Always check for errors and handle them
- Use `context.Context` for request-scoped values, cancellation, deadlines

### Testing
- Use Go's built-in testing framework
- Write tests for all public functions and methods
- Use table-driven tests for multiple scenarios
- Use `mockgen` to generate mocks; use `go generate` directives

### Code Generation
- Use `go generate` for code generation tasks
- NEVER modify generated code manually

### Building
- When running `go build`, prefix output binary with `__tmp_`

### Reference
- When in doubt, consult https://go.dev/doc/effective_go
