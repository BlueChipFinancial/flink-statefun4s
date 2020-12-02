---
layout: docs
title: Intro
permalink: docs/
---

# Flink Statefun4s

This library provides a Scala SDK for remote Flink Stateful Functions.

Flink functions have some state managed by Flink, take and event, and send messages
to other functions.

Flink manages the complexity of dealing with failures and rollback state during exceptional circumstances.
This means that if there is a failure in communication between two functions all function states are rolled back
and retried until it succeeds. This protects from issues where systems can get into a partial state that doesn't rollback
properly in failure scenarios.
