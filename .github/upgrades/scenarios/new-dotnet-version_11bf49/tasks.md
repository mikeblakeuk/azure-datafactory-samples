# DataFactoryLakehouse .NET 10.0 Upgrade Tasks

## Overview

This document tracks the execution of the DataFactoryLakehouse project upgrade from .NET 9.0 to .NET 10.0. The single project will be upgraded atomically with all framework, package, and API compatibility changes applied in one coordinated operation.

**Progress**: 3/4 tasks complete (75%) ![0%](https://progress-bar.xyz/75)

---

## Tasks

### [✓] TASK-001: Verify prerequisites *(Completed: 2026-03-25 14:58)*
**References**: Plan §Phase 0

- [✓] (1) Verify .NET 10 SDK is installed using `dotnet --list-sdks`
- [✓] (2) .NET 10.0.x SDK is available (**Verify**)

---

### [✓] TASK-002: Atomic framework and dependency upgrade with compilation fixes *(Completed: 2026-03-25 15:00)*
**References**: Plan §Phase 1, Plan §Package Update Reference, Plan §Breaking Changes Catalog

- [✓] (1) Update `<TargetFramework>` from net9.0 to net10.0 in DataFactoryLakehouse/DataFactoryLakehouse.csproj
- [✓] (2) Project file updated to net10.0 (**Verify**)
- [✓] (3) Update `Microsoft.Extensions.Hosting` package to version 10.0.5
- [✓] (4) Package reference updated (**Verify**)
- [✓] (5) Run `dotnet restore DataFactoryLakehouse/DataFactoryLakehouse.csproj`
- [✓] (6) Dependencies restored successfully (**Verify**)
- [✓] (7) Build solution and fix all compilation errors per Plan §Breaking Changes (focus: binary incompatible `Configure<T>` method, source incompatible `BinaryData` (7 occurrences), `TimeSpan.FromSeconds` with Int64 parameter)
- [✓] (8) Solution builds with 0 errors (**Verify**)

---

### [✓] TASK-003: Run full test suite and validate upgrade *(Completed: 2026-03-25 15:01)*
**References**: Plan §Phase 2 Testing

- [✓] (1) Execute application using `dotnet run --project DataFactoryLakehouse/DataFactoryLakehouse.csproj` to validate startup and HostBuilder behavioral changes
- [✓] (2) Application starts without exceptions (**Verify**)
- [✓] (3) Test Azure Identity authentication and Azure Data Factory operations
- [✓] (4) All Azure integrations function correctly (**Verify**)

---

### [▶] TASK-004: Final commit
**References**: Plan §Source Control Strategy

- [▶] (1) Commit all changes with message: "Upgrade DataFactoryLakehouse to .NET 10.0\n\n- Update TargetFramework from net9.0 to net10.0\n- Upgrade Microsoft.Extensions.Hosting to 10.0.5\n- Fix binary incompatible API: Configure<T> method\n- Fix source incompatible APIs: BinaryData (7 occurrences), TimeSpan.FromSeconds\n- Document Azure.Identity deprecation (package still functional)\n- Validate HostBuilder behavioral changes\n- All builds succeed with 0 errors\n- All tests pass successfully"

---









