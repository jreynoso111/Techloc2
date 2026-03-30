(function () {
  const supabase = window.supabaseClient;
  
  // Configuration: map each HTML page to its Supabase table
  // Example: if you are on 'control-map.html', it will fetch from 'vehicles'
  const tableMapping = {
    'control-map.html': 'vehicles',
    'locksmiths.html': 'Services',
    'inspectors.html': 'Services',
    'dispatchers.html': 'Services',
    'tires.html': 'Services',
    'services.html': 'Services'
  };

  // Main function to load data
  window.loadSupabaseTable = async function (containerId) {
    // 1. Identify current page and mapped table
    const path = window.location.pathname.split('/').pop(); // e.g. control-map.html
    const tableName = tableMapping[path] || tableMapping['control-map.html']; // Default fallback

    if (!tableName) {
      console.error('No configured table was found for this page:', path);
      return;
    }

    // 2. Fetch data from Supabase
    const { data, error } = await supabase
      .from(tableName)
      .select('*')
      .order('id', { ascending: true }); // assumes ID column exists

    if (error) {
      console.error('Error loading data:', error);
      document.getElementById(containerId).innerHTML = '<p class="error">Error loading data.</p>';
      return;
    }

    // 3. Read current role (stored by authManager.js)
    // Slight delay ensures authManager already set the role
    const userRole = window.currentUserRole || 'user';
    
    // 4. Define permissions
    const canEdit = ['moderator', 'administrator'].includes(userRole);
    const canDelete = userRole === 'administrator';

    console.log(`Loading table: ${tableName} | Role: ${userRole}`);

    // 5. Render table
    renderTable(data, containerId, canEdit, canDelete, tableName);
  };

  function renderTable(data, containerId, canEdit, canDelete, tableName) {
    const container = document.getElementById(containerId);
    if (!data.length) {
      container.innerHTML = '<p>No records found.</p>';
      return;
    }

    // Build table structure
    let html = '<table class="min-w-full divide-y divide-gray-200">';
    
    // -- HEADERS --
    const columns = Object.keys(data[0]).filter(col => col !== 'id' && col !== 'created_at'); // hide technical columns
    html += '<thead class="bg-gray-50"><tr>';
    columns.forEach(col => {
      html += `<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">${col}</th>`;
    });
    // Extra action column for authorized roles
    if (canEdit || canDelete) {
      html += '<th class="px-6 py-3 text-right">Actions</th>';
    }
    html += '</tr></thead>';

    // -- BODY --
    html += '<tbody class="bg-white divide-y divide-gray-200">';
    data.forEach(row => {
      html += '<tr>';
      columns.forEach(col => {
        html += `<td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${row[col] || '-'}</td>`;
      });

      // Action buttons
      if (canEdit || canDelete) {
        html += '<td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">';
        
        if (canEdit) {
          html += `<button onclick="editRecord('${tableName}', '${row.id}')" class="text-indigo-600 hover:text-indigo-900 mr-4">Edit</button>`;
        }
        
        if (canDelete) {
          html += `<button onclick="deleteRecord('${tableName}', '${row.id}')" class="text-red-600 hover:text-red-900">Delete</button>`;
        }
        
        html += '</td>';
      }
      html += '</tr>';
    });
    html += '</tbody></table>';

    container.innerHTML = html;
  }

  // --- Global button handlers ---
  
  window.deleteRecord = async (table, id) => {
    if (!confirm('Are you sure you want to delete this record? This action cannot be undone.')) return;
    
    const { error } = await supabase.from(table).delete().eq('id', id);
    
    if (error) alert('Delete error: ' + error.message);
    else {
      alert('Record deleted');
      location.reload(); // reload to reflect changes
    }
  };

  window.editRecord = (table, id) => {
    alert(`This is where you would open the edit modal for ID: ${id} in table ${table}`);
    // Hook your modal logic here
  };

})();
