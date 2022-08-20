using ProductosVendidos.Models;
using System.Data.SqlClient;
using System.Data;

namespace ProductosVendidosHandlers
{
    public static class ProductoVendidoHandler
    {
        public const string Connection_String = "Server=ARASALP220944\\LOCALDB;Database=SistemaGestion;Trusted_Connection=True";

        //Este método va a buscar los productos vendidos según el IDUsuario y lo devuelve en una tabla
        public static List<ProductoVendido> BuscarProductosVendidos(int IDUsuario)
        {
            //Tabla temporal para almacenarar el resultado de la consulta
            DataTable temp_table = new DataTable();

            List<ProductoVendido> Lista_productos = new List<ProductoVendido>();

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
                        ProductoVendido obj_producto = new ProductoVendido();
                        obj_producto.IdUsuario = Convert.ToInt32(line["IdUsuario"]);
                        obj_producto.IdProducto = Convert.ToInt32(line["IdProducto"]);
                        obj_producto.Descripciones = line["Descripciones"].ToString();
                        obj_producto.Stock = Convert.ToInt32(line["Stock"]);
                        obj_producto.PrecioVenta = Convert.ToDouble(line["PrecioVenta"]);
                        Lista_productos.Add(obj_producto);
                    }
                    sqlConnection.Close();
                }
            }
            return Lista_productos;
        }
    }
    }
