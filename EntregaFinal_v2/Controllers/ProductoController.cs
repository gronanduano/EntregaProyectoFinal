using Microsoft.AspNetCore.Mvc;
using Productos.Models;
using ProductoHandlers;
using Productos.DTOS;

namespace ProductoControllers
{
    [ApiController]
    [Route("[controller]")]

    public class ProductoController : ControllerBase
    {
        [HttpGet(Name = "GetProducto")]
        
        public List<Producto> ObtenerProductos()
        {
            return ProductoHandler.ObtenerProductos();
        }

        [HttpPost(Name = "PostProducto")]
        public List<PostProducto> CrearProducto([FromBody] List<PostProducto> ListadoProductos)
        {
            return ProductoHandler.InsertarProductos(ListadoProductos);
        }
        
        [HttpPut(Name = "PutProducto")]
        public string ModificarProducto([FromBody] PutProducto producto)
        {
            return ProductoHandler.ModificarProducto(new Producto
            {
                Id = producto.ID,
                Descripciones = producto.Descripciones,
                Costo = producto.Costo,
                PrecioVenta = producto.PrecioVenta,
                Stock = producto.Stock,
                IdUsuario = producto.IdUsuario
            });
        }

        [HttpDelete(Name = "DeleteProducto")]
        public string EliminarProducto([FromHeader] int producto)
        {
            return ProductoHandler.EliminarProducto(producto);
        }
    }
}