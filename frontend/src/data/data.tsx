

export const loadTables = async () => {
  try {
    const response = await fetch(`http://127.0.0.1:8000/tables`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });

    if (!response.ok) {
      return { success: false, error: response.status };
    }

    const data = await response.json();
    return { success: true, data: data };
  } catch (err: any) {
    return { success: false, error: err.message };
  }
};

export const execQuery = async (query: {content: string}) => {
  try {
    const response = await fetch(`http://127.0.0.1:8000/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify(query)
    });

    if (!response.ok) {
      return { success: false, error: response.status };
    }

    const data = await response.json();
    return { success: true, data: data };
  } catch (err: any) {
    return { success: false, error: err.message };
  }
};
