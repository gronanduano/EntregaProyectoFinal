using Ventas.Models;
using System.Data.SqlClient;
using System.Data;
using Ventas.DTOS;
using ProductosVendidos.Models;

namespace VentaHandlers
{
    public static class VentaHandler
    {
        public const string Connection_String = "Server=ARASALP220944\\LOCALDB;Database=SistemaGestion;Trusted_Connection=True";

        //Este método va a calcular el total de ventas según un IDUsuario y los devuelve en una tabla
        public static List<Venta> BuscarVentasTotales(int IDUsuario)
        {
            //Tabla temporal para almacenarar el resultado de la consulta
            DataTable temp_table = new DataTable();

            List<Venta> Lista_Ventas = new List<Venta>();

            using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
            {
                using (SqlCommand sqlCommand = new SqlCommand("select p.IdUsuario, pv.IdProducto, p.Descripciones, pv.Stock, p.PrecioVenta " +
                    "from ProductoVendido pv join Producto p on pv.IdProducto = p.Id " +
                    "where p.IdUsuario = @IDUsuario", sqlConnection))
                {
                    sqlConnection.Open();

                    sqlCommand.Parameters.AddWithValue("@IDUsuario", IDUsuario);
                    SqlDataAdapter SqlAdapter = new SqlDataAdapter();
                    SqlAdapter.SelectCommand = sqlCommand;
                    SqlAdapter.Fill(temp_table);

                    foreach (DataRow line in temp_table.Rows)
                    {
                        Venta obj_venta = new Venta();
                        obj_venta.IdUsuario = Convert.ToInt32(line["IdUsuario"]);
                        obj_venta.IdProducto = Convert.ToInt32(line["IdProducto"]);
                        obj_venta.Descripciones = line["Descripciones"].ToString();
                        obj_venta.Stock = Convert.ToInt32(line["Stock"]);
                        obj_venta.PrecioVenta = Convert.ToDouble(line["PrecioVenta"]);
                        obj_venta.Valor_Venta = obj_venta.Stock * obj_venta.PrecioVenta;
                        Lista_Ventas.Add(obj_venta);
                    }
                    sqlConnection.Close();
                }
            }
            return Lista_Ventas;
        }


        /* Este método va a crear una venta según IDProducto, IDUsuario y Cantidad Vendida
        1. Primero se va a validar que el Producto exista
        2. Después se valida que haya stock disponible de ese Producto para vender
        3. Después se valida que el Usuario exista
        3. Se registran las ventas en las tablas: A) Venta B) ProductoVendido y por último C) Se actualiza Producto con el nuevo stock
        4. El método retorna una tabla con los datos de la venta y una columna con el status del registro */
        public static List<PostVenta> InsertarVentas(List<PostVenta> DetalleVenta)
        {
            DataTable dtProductos = new DataTable();
            DataTable dtUsuarios = new DataTable();
            DataRow[] singlequery;
            string query = string.Empty;
            int IDVenta;
            int stock_producto = 0;
            int cont = -1;

            //Buscamos todos los ID de Producto y Stock para no hacer un select por cada item de venta
            dtProductos = Obtener_Stock_Producto();

            //Buscamos todos los ID de Usuario para no hacer un select por cada item de venta
            dtUsuarios = Obtener_IDUsuarios();

            //Se recorren los datos de las ventas recibidas por la API
            foreach (var line in DetalleVenta)
            {
                cont++;
                
                //Validar que el ID de Producto exista y que haya stock suficiente para registrar la venta
                query = "Id = " + line.IdProducto.ToString();
                singlequery = dtProductos.Select(query);

                if (singlequery.Length == 0)
                {
                    DetalleVenta[cont].Status = "Venta no Registrada - No existe el producto";
                    continue;
                }
                else
                {
                    if (line.Stock > Convert.ToInt32(singlequery[0].ItemArray[1]))
                    {
                        DetalleVenta[cont].Status = "Venta no Registrada - No hay Stock suficiente del producto";
                        continue;
                    }
                    else
                    {
                        stock_producto = Convert.ToInt32(singlequery[0].ItemArray[1]) - line.Stock;
                    }
                }

                //Validar que el ID de Usuario exista
                query = "Id = " + line.IdUsuario.ToString();
                singlequery = dtUsuarios.Select(query);

                if (singlequery.Length == 0)
                {
                    DetalleVenta[cont].Status = "Venta no Registrada - No existe el Usuario";
                    continue;
                }

                //Insertar en la tabla Venta (Id automatico y Comentarios: DateTime + IdUsuario vendedor)
                DetalleVenta[cont].Status = Insertar_Tabla_Ventas(line.IdProducto, line.IdUsuario);

                //Si la venta fue registrada sin errores, recuperar el ID de Venta generado
                if (DetalleVenta[cont].Status == "OK")
                {
                    IDVenta = Obtener_IDVenta_generado();
                    DetalleVenta[cont].Status = "Venta Registrada - Id Venta: " + IDVenta + " - IdUsuario: " + line.IdUsuario;
                }
                else
                {
                    continue;
                }

                //Insertar en la tabla Producto vendido
                DetalleVenta[cont].Status = Insertar_Tabla_ProductoVendido(line.IdProducto, line.Stock, IDVenta);

                //Si el ProductoVendido fue registrado sin errores, actualizar el status
                if (DetalleVenta[cont].Status == "OK")
                {
                    DetalleVenta[cont].Status = "Venta Registrada - Id Venta: " + IDVenta + " - IdUsuario: " + line.IdUsuario;
                }
                else
                {
                    continue;
                }

                //Modificar la tabla Producto (Descontar stock))
                DetalleVenta[cont].Status = Actualizar_Stock_Producto(line.IdProducto, stock_producto, IDVenta, line.IdUsuario);

                //Si el Stock fue actualizado sin errores, actualizar el status
                if (DetalleVenta[cont].Status == "OK")
                {
                    DetalleVenta[cont].Status = "Venta Registrada y Stock Actualizado - Id Venta: " + IDVenta + " - IdUsuario: " + line.IdUsuario;
                }
                else
                {
                    continue;
                }
            }
            return DetalleVenta;
        }

        /* Este método va a eliminar una Venta según ID y retorna un texto con el resultado 
        1. Antes de eliminar de ProductoVendido capturar el stock de la venta
        2. Eliminar de ProductoVendido
        3. Actualizar el stock de la tabla Producto
        4. Después se debe eliminar de Venta */
        public static string EliminarVenta(int IDVenta)
        {
            //En este String vamos a capturar el resultado para devolver si se pudo modificar o no
            string Response = String.Empty;
            int stock_vendido = 0;
            int producto_vendido = 0;

            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
                {
                    //Obtener la cantidad vendida antes de eliminar de ProductoVendido para poder actualizar el Stock más adelante
                    Buscar_Producto_Stock(IDVenta, ref producto_vendido, ref stock_vendido);

                    //Primero se deben eliminar los datos de la tabla producto_vendido porque tiene un FK
                    Eliminar_ProductoVendido(IDVenta);

                    //Modificar la tabla producto (Ajustar stock)
                    Response = Agregar_Stock_Producto(producto_vendido, stock_vendido, IDVenta);

                    //Eliminar información de la tabla Venta
                    Response = Eliminar_Tabla_Venta(IDVenta);
                }
            }
            catch (Exception ex)
            {
                Response = "El ID de venta: '" + IDVenta + "' no se ha podido eliminar. Detalle Error: " + ex.Message;
            }
            return Response;
        }

        //Método privado para buscar todos los ID de Producto y Stock
        private static DataTable Obtener_Stock_Producto()
        {
            DataTable dtProd = new DataTable();

            using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
            {
                SqlDataAdapter SqlAdapter = new SqlDataAdapter("SELECT Id, Stock FROM Producto", sqlConnection);
                sqlConnection.Open();
                SqlAdapter.Fill(dtProd);
                sqlConnection.Close();
            }
            return dtProd;
        }

        //Método privado para buscar todos los ID de Usuario
        private static DataTable Obtener_IDUsuarios()
        {
            DataTable dtUsu = new DataTable();
            using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
            {
                SqlDataAdapter SqlAdapter = new SqlDataAdapter("SELECT Id FROM Usuario", sqlConnection);
                sqlConnection.Open();
                SqlAdapter.Fill(dtUsu);
                sqlConnection.Close();
            }
            return dtUsu;
        }

        //Método privado para insertar registros en la tabla Ventas
        private static string Insertar_Tabla_Ventas(int IdProd, int IdUsu)
        {
            string Status = String.Empty;
            int registros_insertados = 0;
            DataTable dtIdVenta = new DataTable();

            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
                {
                    string QueryUpdate = "INSERT INTO Venta ( Comentarios ) VALUES ( @Comentarios )";

                    //Parámetros
                    SqlParameter param_Comentarios = new SqlParameter("Comentarios", SqlDbType.VarChar) { Value = "Venta Registrada - Fecha Operación: " + DateTime.Now + 
                        " - Producto: " + IdProd + " - Vendedor: " + IdUsu };

                    sqlConnection.Open();
                    using (SqlCommand sqlCommand = new SqlCommand(QueryUpdate, sqlConnection))
                    {
                        sqlCommand.Parameters.Add(param_Comentarios);
                        registros_insertados = sqlCommand.ExecuteNonQuery();
                    }
                    if (registros_insertados == 1)
                    {
                        Status = "OK";
                    }
                    else
                    {
                        Status = "Venta No Registrada - Error al ingresar venta";
                    }
                    sqlConnection.Close();
                }
            }
            catch (Exception ex)
            {
                Status = "Venta No Registrada - Error al ingresar venta: " + ex.Message;
            }
            return Status;
        }

        //Método privado para obtener el ID de Venta generado después del insert anterior
        private static int Obtener_IDVenta_generado()
        {
            DataTable dtIdVenta = new DataTable();
            string Status = String.Empty;

            //Obtener IDVenta generado
            using (SqlConnection sqlConnection_id = new SqlConnection(Connection_String))
            {
                sqlConnection_id.Open();
                SqlDataAdapter SqlAdapter = new SqlDataAdapter("SELECT MAX(Id) FROM Venta", sqlConnection_id);
                SqlAdapter.Fill(dtIdVenta);
                sqlConnection_id.Close();
            }
            return Convert.ToInt32(dtIdVenta.Rows[0].ItemArray[0]);
        }

        //Método privado para insertar registros en la tabla ProductoVendido
        private static string Insertar_Tabla_ProductoVendido(int IdProd, int Cant_Vend, int IDVen)
        {
            string Status = String.Empty;
            int registros_insertados = 0;

            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
                {
                    string QueryInsert = "INSERT INTO ProductoVendido ( Stock, IdProducto, IdVenta ) VALUES ( @Stock, @IdProducto, @IdVenta )";

                    //Parámetros
                    SqlParameter param_Stock = new SqlParameter("Stock", SqlDbType.Int) { Value = Cant_Vend };
                    SqlParameter param_IdProducto = new SqlParameter("IdProducto", SqlDbType.Int) { Value = IdProd };
                    SqlParameter param_IdVenta = new SqlParameter("IdVenta", SqlDbType.Int) { Value = IDVen };

                    sqlConnection.Open();
                    using (SqlCommand sqlCommand = new SqlCommand(QueryInsert, sqlConnection))
                    {
                        sqlCommand.Parameters.Add(param_Stock);
                        sqlCommand.Parameters.Add(param_IdProducto);
                        sqlCommand.Parameters.Add(param_IdVenta);
                        registros_insertados = sqlCommand.ExecuteNonQuery();
                        sqlCommand.Parameters.Clear();
                    }
                    if (registros_insertados == 1)
                    {
                        Status = "OK";
                    }
                    else
                    {
                        Status = "Venta No Registrada - Error al ingresar venta";
                    }
                    sqlConnection.Close();
                }
            }
            catch (Exception ex)
            {
                Status = "Venta No Registrada - Error al ingresar venta: " + ex.Message;
            }
            return Status;
        }

        //Método privado para actualizar el stock en la tabla Producto
        private static string Actualizar_Stock_Producto(int Prod, int Stock, int IDVen, int IDUser)
        {
            string Status = String.Empty;
            int registros_actualizados = 0;

            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
                {
                    string QueryUpdate = "UPDATE Producto SET Stock = " + Stock + " WHERE Id = @IdProducto";

                    //Parámetros
                    SqlParameter param_IdProducto = new SqlParameter("IdProducto", SqlDbType.Int) { Value = Prod };

                    sqlConnection.Open();
                    using (SqlCommand sqlCommand = new SqlCommand(QueryUpdate, sqlConnection))
                    {
                        sqlCommand.Parameters.Add(param_IdProducto);
                        registros_actualizados = sqlCommand.ExecuteNonQuery();
                        sqlCommand.Parameters.Clear();
                    }
                    if (registros_actualizados == 1)
                    {
                        Status = "OK";
                    }
                    else
                    {
                        Status = "Venta No Registrada - Error al ingresar venta";
                    }
                    sqlConnection.Close();
                }
            }
            catch (Exception ex)
            {
                Status = "Venta No Registrada - Error al ingresar venta: " + ex.Message;
            }
            return Status;
        }

        //Método privado para Buscar Producto y Stock según IDVenta
        private static void Buscar_Producto_Stock(int IDVen, ref int IDProd, ref int Stock)
        {
            //Tabla temporal para almacenar el resultado de la consulta
            DataTable temp_table = new DataTable();

            using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
            {
                string QueryStockVendido = "SELECT IdProducto, Stock FROM ProductoVendido WHERE IDVenta = @IDVenta";
                sqlConnection.Open();
                using (SqlCommand sqlCommand = new SqlCommand(QueryStockVendido, sqlConnection))
                {
                    sqlCommand.Parameters.AddWithValue("@IDVenta", IDVen);
                    SqlDataAdapter SqlAdapter = new SqlDataAdapter();
                    SqlAdapter.SelectCommand = sqlCommand;
                    SqlAdapter.Fill(temp_table);

                    if (temp_table.Rows.Count > 0)
                    {
                        foreach (DataRow line in temp_table.Rows)
                        {
                            Stock = Convert.ToInt32(line["Stock"]);
                            IDProd = Convert.ToInt32(line["IdProducto"]);
                            //Considero que va a haber un único registro
                            break;
                        }
                    }
                }
                sqlConnection.Close();
            }
        }

        //Método privado para eliminar de la tabla ProductoVendido
        private static void Eliminar_ProductoVendido(int IDVen)
        {
            int registros_eliminados = 0;

            using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
            { 
                string QueryDelete_prodvendido = "DELETE FROM ProductoVendido WHERE IDVenta = @IDVenta";
                SqlParameter param_IDVenta = new SqlParameter("IDVenta", System.Data.SqlDbType.BigInt) { Value = IDVen };
                sqlConnection.Open();
                using (SqlCommand sqlCommand = new SqlCommand(QueryDelete_prodvendido, sqlConnection))
                {
                    sqlCommand.Parameters.Add(param_IDVenta);
                    registros_eliminados = sqlCommand.ExecuteNonQuery();
                    sqlCommand.Parameters.Clear();
                }
                sqlConnection.Close();
            }
        }

        //Método privado para Agregar el stock del Producto eliminado
        private static string Agregar_Stock_Producto(int Prod_vend, int Stock_vend, int IDVen)
        {
            string Status = String.Empty;
            int registros_eliminados;

            using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
            {
                string QueryUpdate_stock = "UPDATE Producto SET stock = ( stock + " + Stock_vend + ") where Id = @IDProducto";
                SqlParameter param_IDProducto = new SqlParameter("IDProducto", System.Data.SqlDbType.Int) { Value = Prod_vend };

                sqlConnection.Open();
                using (SqlCommand sqlCommand = new SqlCommand(QueryUpdate_stock, sqlConnection))
                {
                    sqlCommand.Parameters.Add(param_IDProducto);
                    registros_eliminados = sqlCommand.ExecuteNonQuery();
                }
                if (registros_eliminados == 1)
                {
                    Status = "El ID de venta: '" + IDVen + "' se ha eliminado correctamente.";
                }
                else
                {
                    Status = "El ID de venta: '" + IDVen + "' no se ha podido eliminar.";
                }
                sqlConnection.Close();

            }
            return Status;
        }

        //Método privado para eliminar de la tabla Ventas
        private static string Eliminar_Tabla_Venta(int IDVen)
        {
            string Status = String.Empty;
            int registros_eliminados;

            using (SqlConnection sqlConnection = new SqlConnection(Connection_String))
            {
                string QueryDeleteVenta = "DELETE FROM Venta WHERE Id = @IDVenta";
                SqlParameter param_IDVenta = new SqlParameter("IDVenta", System.Data.SqlDbType.BigInt) { Value = IDVen };
                sqlConnection.Open();
               
                using (SqlCommand sqlCommand = new SqlCommand(QueryDeleteVenta, sqlConnection))
                {
                    sqlCommand.Parameters.Add(param_IDVenta);
                    registros_eliminados = sqlCommand.ExecuteNonQuery();
                }
                if (registros_eliminados == 1)
                {
                    Status = "El ID de venta: '" + IDVen + "' se ha eliminado correctamente.";
                }
                else
                {
                    Status = "El ID de venta: '" + IDVen + "' no se ha podido eliminar.";
                }
                sqlConnection.Close();

            }
            return Status;
        }
    }
}
