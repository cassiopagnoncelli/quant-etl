import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
  static targets = ["toggle", "status"]
  static values = { 
    pipelineId: Number,
    url: String 
  }

  connect() {
    console.log("Individual pipeline toggle controller connected")
  }

  async handleToggle(event) {
    const toggle = event.target
    const willActivate = toggle.checked
    
    // Show loading state
    this.showLoadingState(toggle)
    
    try {
      // Create a form data object for the PATCH request
      const formData = new FormData()
      formData.append('_method', 'PATCH')
      
      const response = await fetch(this.urlValue, {
        method: 'POST',
        headers: {
          'X-CSRF-Token': this.getCSRFToken(),
          'Accept': 'text/html'
        },
        body: formData
      })
      
      if (response.ok) {
        // Update the status text immediately
        this.updateStatusText(willActivate)
        
        // Show success message
        this.showMessage(`Pipeline ${willActivate ? 'activated' : 'deactivated'} successfully`, 'success')
        
        // Reload page after short delay to update all UI elements
        setTimeout(() => {
          window.location.reload()
        }, 1000)
        
      } else {
        throw new Error('Failed to toggle pipeline status')
      }
      
    } catch (error) {
      console.error('Error toggling pipeline:', error)
      
      // Revert toggle state
      toggle.checked = !willActivate
      
      // Show error message
      this.showMessage(`Error: ${error.message}`, 'error')
      
    } finally {
      // Remove loading state
      this.hideLoadingState(toggle)
    }
  }

  showLoadingState(toggle) {
    const toggleSwitch = toggle.closest('.toggle-switch')
    toggleSwitch.classList.add('loading')
    toggle.disabled = true
  }

  hideLoadingState(toggle) {
    const toggleSwitch = toggle.closest('.toggle-switch')
    toggleSwitch.classList.remove('loading')
    toggle.disabled = false
  }

  updateStatusText(isActive) {
    if (this.hasStatusTarget) {
      this.statusTarget.textContent = isActive ? 'Active' : 'Inactive'
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

  getCSRFToken() {
    const token = document.querySelector('meta[name="csrf-token"]')
    return token ? token.getAttribute('content') : ''
  }
}
