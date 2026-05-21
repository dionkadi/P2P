# Plan: Push P2P Project to Production-Ready BitTorrent Client

## TL;DR
> **Summary**: Transform the existing C++23 P2P codebase into a production-ready BitTorrent CLI client that interoperates with real-world clients (Transmission, qBittorrent). Focus on protocol compliance, networking hardening, connection management, multi-torrent support, and comprehensive testing.
> **Deliverables**: `p2p` CLI binary (seed/download/create), `p2p_test` with full test suite, `libp2p_common` production library
> **Effort**: Large (20+ TODOs across 6 waves)
> **Parallel**: YES - 4 parallelizable waves + 2 sequential waves
> **Critical Path**: Wave 1 (protocol compliance + build) → Wave 4 (config) → Wave 5 (multi-torrent) → Wave 6 (tracker) → Wave 7 (verification)

## Context
### Original Request
"i'd like to push my project to a real-world bittorrent application. what the plan should be?"

### Interview Summary
- **Deliverable**: Standalone CLI client (`p2p` binary) like Transmission-cli
- **Platform**: Linux only
- **Interoperability**: MUST work with real BitTorrent clients (full BEP compliance)
- **Test Strategy**: TDD for new features; enable and fix existing unit tests
- **Key Discovery**: Protocol string is `"MIT-P2P-V1.0"` — non-standard, must change to `"BitTorrent protocol"` for interop

## Work Objectives
### Core Objective
Ship a production-quality CLI BitTorrent client that can seed/download via `.torrent` files and magnet links, interoperate with mainstream clients, and handle real-world network conditions (timeouts, bad peers, connection limits).

### Definition of Done (verifiable conditions with commands)
- `cmake -B build && cmake --build build` succeeds with zero errors (`-Werror`)
- `./build/p2p_test` passes all tests
- All unit tests enabled and passing

### Must Have
- Standard BitTorrent protocol string for handshake compatibility
- Connection limits (max total, max per torrent, max half-open)
- Block request timeout (cancel + re-request after N seconds)
- Retry with exponential backoff for failed connections and tracker announces
- Peer banning for misbehaving peers (corrupt data, protocol violations)
- All unit tests enabled and passing

### Must NOT Have (guardrails)
- NO uTP (BEP 29) — too complex, out of scope
- NO protocol encryption (BEP 27) — out of scope
- NO GUI/Web UI — CLI only
- NO proxy support — out of scope
- NO cross-platform support — Linux only
- DO NOT rewrite working code for style preferences
- DO NOT refactor unless it directly enables a required feature

## TODOs

- [x] 1.1. Fix BitTorrent Protocol Compliance for Interoperability
- [x] 1.2. Enable and Fix Client and Tracker Executables
- [x] 1.3. Enable and Fix All Unit Tests
- [x] 1.4. Implement Connection Limits in PeerManager
- [x] 1.5. Code Cleanup (Duplicate Includes, Declarations)

- [x] 2.1. Implement Block Request Timeouts in PieceManager
- [x] 2.2. Implement Retry with Exponential Backoff
- [x] 2.3. Implement Peer Banning Mechanism
- [x] 2.4. Implement Per-Peer Rate Limiting

- [x] 3.1. Implement Disk Cache in FileManager
- [x] 3.2. Implement Fast Extension (BEP 6)
- [x] 3.3. Implement Local Peer Discovery (BEP 14)

- [x] 4.1. Create Configuration System
- [x] 4.2. Create Multi-Torrent ClientApp Manager

- [x] 5.1. Refine Choking Algorithm (BEP 3 Tit-for-Tat)
- [x] 5.2. Implement Request Pipelining Limits

- [x] 6.1. Fix Tracker Seeder/Leecher Counts and Add Peer Expiration

## Final Verification Wave
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high
- [ ] F4. Scope Fidelity Check — deep
