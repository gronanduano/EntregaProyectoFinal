namespace Productos.DTOS
{
    //Intercambio de valores para MODIFICAR (PUT) un producto
    public class PutProducto
    {
        public int ID { get; set; }
        public string Descripciones { get; set; }
        public double Costo { get; set; }
        public double PrecioVenta { get; set; }
        public int Stock { get; set; }
        public int IdUsuario { get; set; }
    }
}
