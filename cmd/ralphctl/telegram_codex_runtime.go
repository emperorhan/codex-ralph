package main

const telegramCodexTimeoutCapSec = 1800

func resolveTelegramCodexTimeoutSec(configured, fallback int) int {
	timeoutSec := configured
	if timeoutSec <= 0 {
		timeoutSec = fallback
	}
	if timeoutSec <= 0 {
		timeoutSec = 60
	}
	if timeoutSec > telegramCodexTimeoutCapSec {
		timeoutSec = telegramCodexTimeoutCapSec
	}
	return timeoutSec
}

