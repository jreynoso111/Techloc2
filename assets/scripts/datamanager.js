(function () {
  const supabase = window.supabaseClient;
  
  // Configuración: Mapea el nombre de tu archivo HTML con la tabla de Supabase
  // Ejemplo: si estás en 'control-map.html', buscará datos en la tabla 'vehicles'
  const tableMapping = {
    'control-map.html': 'vehicles',
    'locksmiths.html': 'Services',
    'inspectors.html': 'Services',
    'dispatchers.html': 'Services',
    'tires.html': 'Services',
    'services.html': 'Services'
  };

  // Función principal para cargar datos
  window.loadSupabaseTable = async function (containerId) {
    // 1. Identificar en qué página estamos y qué tabla toca
    const path = window.location.pathname.split('/').pop(); // ej: control-map.html
    const tableName = tableMapping[path] || tableMapping['control-map.html']; // Default fallback

    if (!tableName) {
      console.error('No se encontró una tabla configurada para esta página:', path);
      return;
    }

    // 2. Obtener datos de Supabase
    const { data, error } = await supabase
      .from(tableName)
      .select('*')
      .order('id', { ascending: true }); // Asumiendo que tienen ID

    if (error) {
      console.error('Error cargando datos:', error);
      document.getElementById(containerId).innerHTML = '<p class="error">Error cargando datos.</p>';
      return;
    }

    // 3. Obtener el rol actual (que guardamos en authManager.js)
    // Esperamos un poco para asegurar que authManager ya definió el rol
    const userRole = window.currentUserRole || 'user';
    
    // 4. Definir Permisos
    const canEdit = ['moderator', 'administrator'].includes(userRole);
    const canDelete = userRole === 'administrator';

    console.log(`Cargando tabla: ${tableName} | Rol: ${userRole}`);

    // 5. Dibujar la Tabla
    renderTable(data, containerId, canEdit, canDelete, tableName);
  };

  function renderTable(data, containerId, canEdit, canDelete, tableName) {
    const container = document.getElementById(containerId);
    if (!data.length) {
      container.innerHTML = '<p>No hay datos registrados.</p>';
      return;
    }

    // Crear estructura de tabla
    let html = '<table class="min-w-full divide-y divide-gray-200">';
    
    // -- ENCABEZADOS (Headers) --
    const columns = Object.keys(data[0]).filter(col => col !== 'id' && col !== 'created_at'); // Ocultamos columnas técnicas
    html += '<thead class="bg-gray-50"><tr>';
    columns.forEach(col => {
      html += `<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">${col}</th>`;
    });
    // Columna extra para acciones si tienes permisos
    if (canEdit || canDelete) {
      html += '<th class="px-6 py-3 text-right">Acciones</th>';
    }
    html += '</tr></thead>';

    // -- CUERPO (Body) --
    html += '<tbody class="bg-white divide-y divide-gray-200">';
    data.forEach(row => {
      html += '<tr>';
      columns.forEach(col => {
        html += `<td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${row[col] || '-'}</td>`;
      });

      // Botones de Acción
      if (canEdit || canDelete) {
        html += '<td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">';
        
        if (canEdit) {
          html += `<button onclick="editRecord('${tableName}', '${row.id}')" class="text-indigo-600 hover:text-indigo-900 mr-4">Editar</button>`;
        }
        
        if (canDelete) {
          html += `<button onclick="deleteRecord('${tableName}', '${row.id}')" class="text-red-600 hover:text-red-900">Borrar</button>`;
        }
        
        html += '</td>';
      }
      html += '</tr>';
    });
    html += '</tbody></table>';

    container.innerHTML = html;
  }

  // --- Funciones Globales para los botones ---
  
  window.deleteRecord = async (table, id) => {
    if (!confirm('¿Estás seguro de borrar este registro? Esta acción no se puede deshacer.')) return;
    
    const { error } = await supabase.from(table).delete().eq('id', id);
    
    if (error) alert('Error al borrar: ' + error.message);
    else {
      alert('Registro borrado');
      location.reload(); // Recargar para ver cambios
    }
  };

  window.editRecord = (table, id) => {
    alert(`Aquí abrirías el modal para editar el ID: ${id} de la tabla ${table}`);
    // Aquí puedes conectar tu lógica de modales más adelante
  };

})();
