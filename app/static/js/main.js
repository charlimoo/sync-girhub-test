// app/static/js/main.js
document.addEventListener('DOMContentLoaded', function() {
    
    // --- Logic for Log Details Modal ---
    const logDetailsModal = document.getElementById('logDetailsModal');
    if (logDetailsModal) {
        logDetailsModal.addEventListener('show.bs.modal', function(event) {
            // Button that triggered the modal
            const button = event.relatedTarget;
            // Extract info from data-log-details attribute
            const detailsJsonString = button.getAttribute('data-log-details');
            
            const modalBody = logDetailsModal.querySelector('#logDetailsContent');
            
            try {
                // Parse and nicely format the JSON
                const detailsObject = JSON.parse(detailsJsonString);
                modalBody.textContent = JSON.stringify(detailsObject, null, 2);
            } catch (e) {
                // If it's not JSON (e.g., plain text error), just display it
                modalBody.textContent = detailsJsonString;
            }
        });
    }

    // --- Logic for Schedule Edit Modal ---
    const scheduleModal = document.getElementById('scheduleModal');
    if (scheduleModal) {
        const scheduleForm = document.getElementById('scheduleForm');
        const frequencySelect = document.getElementById('frequency');

        // Event listener to show/hide options based on frequency
        frequencySelect.addEventListener('change', function() {
            document.querySelectorAll('.schedule-options').forEach(el => el.style.display = 'none');
            const selectedOption = this.value;
            if (selectedOption === 'daily' || selectedOption === 'hourly') {
                document.getElementById('daily-options').style.display = 'block';
            } else if (selectedOption === 'weekly') {
                document.getElementById('daily-options').style.display = 'block';
                document.getElementById('weekly-options').style.display = 'block';
            } else if (selectedOption === 'custom') {
                document.getElementById('custom-options').style.display = 'block';
            }
        });

        // Event listener to set the form action when modal opens
        scheduleModal.addEventListener('show.bs.modal', function(event) {
            const button = event.relatedTarget;
            const jobId = button.getAttribute('data-job-id');
            scheduleForm.action = `/job/update_schedule/${jobId}`;
        });

        // Trigger change on load to set initial state
        frequencySelect.dispatchEvent(new Event('change'));
    }
});