def calculate_trust_score(pages_conf: list, lang_ok: bool, noise_ratio: float) -> float:
    # Basit ama genişletilebilir: ortalama confidence + dil bonusu - gürültü cezası
    if not pages_conf:
        return 0.0
    avg = sum(pages_conf) / max(len(pages_conf), 1)
    score = avg
    if lang_ok:
        score += 0.05
    score -= min(0.20, noise_ratio * 0.5)
    return max(0.0, min(1.0, score))
