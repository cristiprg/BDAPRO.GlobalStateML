package de.tu_berlin.dima.bdapro.cli.command

import java.nio.file.Paths

import org.springframework.stereotype.Service

/** Verify and merge the warmup tasks into the current branch. */
@Service("merge:gameoflife")
class MergeGameOfLife extends MergeTask {

  def commitMsg(user: String): String =
    s"[WARMUP] $taskName solution from '$user'."

  override val taskName = "Game Of Life"

  override val taskBranch = "gameoflife"
}
