import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
  static targets = ["toggle", "content"]
  static values = { 
    url: String, 
    interval: { type: Number, default: 5000 },
    enabled: { type: Boolean, default: false }
  }

  connect() {
    this.intervalId = null
    this.isUpdating = false
    
    // Set initial toggle state
    if (this.hasToggleTarget) {
      this.toggleTarget.checked = this.enabledValue
    }
    
    // Start polling if enabled by default
    if (this.enabledValue) {
      this.startPolling()
    }
  }

  disconnect() {
    this.stopPolling()
  }

  toggle() {
    if (this.toggleTarget.checked) {
      this.startPolling()
    } else {
      this.stopPolling()
    }
  }

  startPolling() {
    if (this.intervalId) return // Already polling
    
    this.intervalId = setInterval(() => {
      this.updateContent()
    }, this.intervalValue)
    
    // Also update immediately
    this.updateContent()
  }

  stopPolling() {
    if (this.intervalId) {
      clearInterval(this.intervalId)
      this.intervalId = null
    }
  }

  async updateContent() {
    if (this.isUpdating) return // Prevent concurrent updates
    
    this.isUpdating = true
    
    try {
      const response = await fetch(this.urlValue, {
        headers: {
          'Accept': 'application/json',
          'X-Requested-With': 'XMLHttpRequest'
        }
      })
      
      if (response.ok) {
        const data = await response.json()
        this.updateUI(data)
      }
    } catch (error) {
      console.error('Live update failed:', error)
    } finally {
      this.isUpdating = false
    }
  }

  updateUI(data) {
    // Check if this is a pipeline list update (pipelines index page)
    if (data.pipelines && Array.isArray(data.pipelines)) {
      this.updatePipelinesList(data.pipelines)
      // Check if any pipeline has completed and stop polling if needed
      this.checkForCompletedPipelines(data.pipelines)
    } else {
      // Single pipeline/run update
      this.updateStatusBadges(data)
      this.updateStatistics(data)
      this.updateRunsCounts(data)
      this.updateLogs(data)
      this.updateProgress(data)
      this.updateTimestamps(data)
      // Check if this single pipeline has completed
      this.checkForCompletedPipeline(data)
    }
  }

  updatePipelinesList(pipelines) {
    pipelines.forEach(pipeline => {
      // Find the pipeline row by ID
      const pipelineRow = this.element.querySelector(`[data-pipeline-id="${pipeline.id}"]`)
      if (pipelineRow) {
        // Update status badges
        const statusBadge = pipelineRow.querySelector('.status-badge')
        if (statusBadge && pipeline.status) {
          statusBadge.className = `status-badge status-${pipeline.status.toLowerCase()}`
          statusBadge.textContent = pipeline.status.charAt(0).toUpperCase() + pipeline.status.slice(1).toLowerCase()
        }

        // Update stage badges
        const stageBadge = pipelineRow.querySelector('.stage-badge')
        if (stageBadge && pipeline.stage) {
          stageBadge.className = `stage-badge stage-${pipeline.stage.toLowerCase()}`
          stageBadge.textContent = pipeline.stage.charAt(0).toUpperCase() + pipeline.stage.slice(1).toLowerCase()
        }

        // Update latest timestamp
        const latestTimestamp = pipelineRow.querySelector('.latest-timestamp')
        if (latestTimestamp && pipeline.latest_timestamp) {
          latestTimestamp.textContent = pipeline.latest_timestamp
        }

        // Update run counts
        if (pipeline.runs_count) {
          const totalRunsElement = pipelineRow.querySelector('[data-stat="total-runs"]')
          if (totalRunsElement) {
            totalRunsElement.textContent = pipeline.runs_count.total
          }

          const completedRunsElement = pipelineRow.querySelector('[data-stat="completed-runs"]')
          if (completedRunsElement) {
            completedRunsElement.textContent = pipeline.runs_count.completed
          }

          const failedRunsElement = pipelineRow.querySelector('[data-stat="failed-runs"]')
          if (failedRunsElement) {
            failedRunsElement.textContent = pipeline.runs_count.failed
          }
        }
      }
    })
  }

  updateStatusBadges(data) {
    // Update status badges
    const statusBadges = this.element.querySelectorAll('.status-badge')
    statusBadges.forEach(badge => {
      if (data.status) {
        badge.className = `status-badge status-${data.status.toLowerCase()}`
        badge.textContent = data.status.charAt(0).toUpperCase() + data.status.slice(1).toLowerCase()
      }
    })
    
    // Update stage badges
    const stageBadges = this.element.querySelectorAll('.stage-badge')
    stageBadges.forEach(badge => {
      if (data.stage) {
        badge.className = `stage-badge stage-${data.stage.toLowerCase()}`
        badge.textContent = data.stage.charAt(0).toUpperCase() + data.stage.slice(1).toLowerCase()
      }
    })
  }

  updateStatistics(data) {
    if (data.statistics) {
      // Update successful count
      const successfulElements = this.element.querySelectorAll('[data-stat="successful"]')
      successfulElements.forEach(el => {
        el.textContent = this.formatNumber(data.statistics.n_successful || 0)
      })
      
      // Update failed count
      const failedElements = this.element.querySelectorAll('[data-stat="failed"]')
      failedElements.forEach(el => {
        el.textContent = this.formatNumber(data.statistics.n_failed || 0)
      })
      
      // Update skipped count
      const skippedElements = this.element.querySelectorAll('[data-stat="skipped"]')
      skippedElements.forEach(el => {
        el.textContent = this.formatNumber(data.statistics.n_skipped || 0)
      })
      
      // Update total count
      const totalElements = this.element.querySelectorAll('[data-stat="total"]')
      totalElements.forEach(el => {
        const total = (data.statistics.n_successful || 0) + 
                     (data.statistics.n_failed || 0) + 
                     (data.statistics.n_skipped || 0)
        el.textContent = this.formatNumber(total)
      })
      
      // Update success rate
      const successRateElements = this.element.querySelectorAll('[data-stat="success-rate"]')
      successRateElements.forEach(el => {
        const total = (data.statistics.n_successful || 0) + 
                     (data.statistics.n_failed || 0) + 
                     (data.statistics.n_skipped || 0)
        const rate = total > 0 ? ((data.statistics.n_successful || 0) / total * 100).toFixed(2) : 0
        el.textContent = `${rate}%`
      })
      
      // Update success rate bar
      const successRateBars = this.element.querySelectorAll('.rate-fill')
      successRateBars.forEach(bar => {
        const total = (data.statistics.n_successful || 0) + 
                     (data.statistics.n_failed || 0) + 
                     (data.statistics.n_skipped || 0)
        const rate = total > 0 ? ((data.statistics.n_successful || 0) / total * 100).toFixed(2) : 0
        bar.style.width = `${rate}%`
      })
    }
  }

  updateRunsCounts(data) {
    if (data.runs_count) {
      // Update total runs count
      const totalRunsElements = this.element.querySelectorAll('[data-stat="total-runs"]')
      totalRunsElements.forEach(el => {
        el.textContent = this.formatNumber(data.runs_count.total || 0)
      })

      // Update completed runs count
      const completedRunsElements = this.element.querySelectorAll('[data-stat="completed-runs"]')
      completedRunsElements.forEach(el => {
        el.textContent = this.formatNumber(data.runs_count.completed || 0)
      })

      // Update failed runs count
      const failedRunsElements = this.element.querySelectorAll('[data-stat="failed-runs"]')
      failedRunsElements.forEach(el => {
        el.textContent = this.formatNumber(data.runs_count.failed || 0)
      })
    }
  }

  updateLogs(data) {
    if (data.logs && data.logs.length > 0) {
      const logsContainer = this.element.querySelector('.logs-container')
      if (logsContainer) {
        // Clear existing logs
        logsContainer.innerHTML = ''
        
        // Add new logs
        data.logs.forEach(log => {
          const logEntry = document.createElement('div')
          logEntry.className = `log-entry ${log.level}`
          logEntry.innerHTML = `
            <div class="log-timestamp">${this.formatTime(log.created_at)}</div>
            <div class="log-level">${log.level.toUpperCase()}</div>
            <div class="log-message">${this.escapeHtml(log.message)}</div>
          `
          logsContainer.appendChild(logEntry)
        })
        
        // Scroll to bottom
        logsContainer.scrollTop = logsContainer.scrollHeight
      }
    }
  }

  updateProgress(data) {
    if (data.stage) {
      const stages = ['START', 'FETCH', 'TRANSFORM', 'IMPORT', 'POST_PROCESSING', 'FINISH']
      const currentStageIndex = stages.indexOf(data.stage)
      
      const progressSteps = this.element.querySelectorAll('.progress-step')
      progressSteps.forEach((step, index) => {
        step.classList.remove('active', 'current')
        
        if (currentStageIndex >= 0) {
          if (index <= currentStageIndex) {
            step.classList.add('active')
          }
          if (index === currentStageIndex) {
            step.classList.add('current')
          }
        }
      })
    }
  }

  updateTimestamps(data) {
    if (data.updated_at) {
      const timestampElements = this.element.querySelectorAll('[data-timestamp="updated"]')
      timestampElements.forEach(el => {
        el.textContent = this.formatDateTime(data.updated_at)
      })
    }
    
    if (data.latest_timestamp) {
      const latestTimestampElements = this.element.querySelectorAll('.latest-timestamp')
      latestTimestampElements.forEach(el => {
        el.textContent = data.latest_timestamp
      })
    }
  }

  formatNumber(num) {
    return new Intl.NumberFormat().format(num)
  }

  formatTime(timestamp) {
    const date = new Date(timestamp)
    return date.toLocaleTimeString('en-US', { 
      hour12: false, 
      hour: '2-digit', 
      minute: '2-digit', 
      second: '2-digit',
      fractionalSecondDigits: 3
    })
  }

  formatDateTime(timestamp) {
    const date = new Date(timestamp)
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true
    })
  }

  escapeHtml(text) {
    const div = document.createElement('div')
    div.textContent = text
    return div.innerHTML
  }

  checkForCompletedPipeline(data) {
    // Check if the pipeline status is COMPLETED or FAILED, or if stage is FINISH
    if (data.status === 'COMPLETED' || data.status === 'FAILED' || data.stage === 'FINISH') {
      console.log(`Pipeline completed with status: ${data.status}, stage: ${data.stage}. Stopping live updates.`)
      this.stopPollingAndDisableToggle()
    }
  }

  checkForCompletedPipelines(pipelines) {
    // For pipeline list view, we don't automatically stop polling since there might be multiple pipelines
    // But we could add logic here if needed for specific scenarios
    // For now, we'll leave this empty but keep the method for future enhancements
  }

  stopPollingAndDisableToggle() {
    // Stop the polling
    this.stopPolling()
    
    // Disable and uncheck the toggle if it exists
    if (this.hasToggleTarget) {
      this.toggleTarget.checked = false
      this.toggleTarget.disabled = true
      
      // Add visual indication that polling has stopped due to completion
      const toggleLabel = this.toggleTarget.closest('.toggle-label')
      if (toggleLabel) {
        toggleLabel.style.opacity = '0.6'
        toggleLabel.title = 'Live updates stopped - pipeline completed'
      }
    }
  }
}
