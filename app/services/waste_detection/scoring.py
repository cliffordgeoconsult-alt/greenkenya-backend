def compute_score(area, change_strength):
    score = 0

    if area > 1000:
        score += 40

    if change_strength > 0.2:
        score += 30

    score += 30

    return min(score, 100)