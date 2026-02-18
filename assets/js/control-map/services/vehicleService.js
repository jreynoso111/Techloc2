export const createVehicleService = ({ client, tableName }) => {
  const listVehicles = async () => {
    if (!client?.from) {
      throw new Error('Vehicle data provider unavailable.');
    }
    const { data, error } = await client.from(tableName).select('*');
    if (error) throw error;
    return data || [];
  };

  return { listVehicles };
};
