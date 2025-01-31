# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-02-01

### Added
- Initial release of Zentask
- Task processing system with NATS JetStream backend
- Support for immediate, delayed, and scheduled tasks
- Task status tracking and monitoring
- Retry mechanism for failed tasks
- Task prioritization
- Task metadata and tagging support
- Progress tracking for long-running tasks
- Built-in logging with Charm logger
- Example implementation in task_demo.go

### Dependencies
- Go 1.22+
- NATS Server v2.10.25
- NATS Go Client v1.38.0
- Cron v3.0.1

## [0.2.0] - 2025-02-01

### Added
- Task processing system with NATS JetStream backend
- Support for immediate, delayed, and scheduled tasks
- Task status tracking and monitoring
- Retry mechanism for failed tasks
- Task prioritization
- Task metadata and tagging support
- Progress tracking for long-running tasks
- Built-in logging with Charm logger
- Example implementation in task_demo.go

### Dependencies
- Go 1.22+
- NATS Server v2.10.25
- NATS Go Client v1.38.0
- Cron v3.0.1
