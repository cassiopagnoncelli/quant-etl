import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
  connect() {
    this.setupToggleListeners()
  }

  setupToggleListeners() {
    const toggles = document.querySelectorAll('.pipeline-toggle')
    
    toggles.forEach(toggle => {
      toggle.addEventListener('change', (event) => {
        event.preventDefault()
        this.handleToggleChange(event.target)
      })
    })
  }

  handleToggleChange(toggle) {
    const source = toggle.dataset.source
    const willActivate = toggle.checked
    
    // Revert the toggle state temporarily
    toggle.checked = !willActivate
    
    // Show confirmation modal
    this.showConfirmationModal(source, willActivate, toggle)
  }

  showConfirmationModal(source, willActivate, toggle) {
    const action = willActivate ? 'activate' : 'deactivate'
    
    const modal = document.createElement('div')
    modal.className = 'pipeline-confirmation-modal'
    modal.innerHTML = `
      <div class="pipeline-confirmation-content">
        <h3>Confirm Pipeline ${action.charAt(0).toUpperCase() + action.slice(1)}</h3>
        <p>
          Are you sure you want to <strong>${action}</strong> all pipelines for source <strong>${source}</strong>?
          <br><br>
          This will affect all time series from this source.
        </p>
        <div class="pipeline-confirmation-buttons">
          <button class="cancel-btn" type="button">Cancel</button>
          <button class="confirm-btn" type="button">Confirm ${action.charAt(0).toUpperCase() + action.slice(1)}</button>
        </div>
      </div>
    `
    
    document.body.appendChild(modal)
    
    // Handle modal buttons
    const cancelBtn = modal.querySelector('.cancel-btn')
    const confirmBtn = modal.querySelector('.confirm-btn')
    
    cancelBtn.addEventListener('click', () => {
      document.body.removeChild(modal)
    })
    
    confirmBtn.addEventListener('click', () => {
      document.body.removeChild(modal)
      this.executeToggle(source, willActivate, toggle)
    })
    
    // Close modal on background click
    modal.addEventListener('click', (event) => {
      if (event.target === modal) {
        document.body.removeChild(modal)
      }
    })
  }

  async executeToggle(source, willActivate, toggle) {
    // Show loading state
    const toggleSwitch = toggle.closest('.toggle-switch')
    toggleSwitch.classList.add('loading')
    toggle.disabled = true
    
    try {
      const response = await fetch('/time_series/toggle_source_pipelines', {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          source: source,
          active: willActivate
        })
      })
      
      const data = await response.json()
      
      if (response.ok && data.success) {
        // Show success message briefly then reload
        this.showMessage(data.message, 'success')
        
        // Reload page after short delay
        setTimeout(() => {
          window.location.reload()
        }, 1000)
        
      } else {
        throw new Error(data.error || 'Failed to toggle pipelines')
      }
      
    } catch (error) {
      console.error('Error toggling pipelines:', error)
      this.showMessage(`Error: ${error.message}`, 'error')
      
      // Remove loading state on error
      toggleSwitch.classList.remove('loading')
      toggle.disabled = false
    }
  }

  showMessage(message, type) {
    const messageDiv = document.createElement('div')
    messageDiv.className = `pipeline-message ${type}`
    messageDiv.textContent = message
    
    document.body.appendChild(messageDiv)
    
    // Auto-remove message after 3 seconds
    setTimeout(() => {
      if (document.body.contains(messageDiv)) {
        document.body.removeChild(messageDiv)
      }
    }, 3000)
  }
}
