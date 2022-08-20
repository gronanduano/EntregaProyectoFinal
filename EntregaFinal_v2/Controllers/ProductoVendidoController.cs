using Microsoft.AspNetCore.Mvc;
using ProductosVendidos.Models;
using ProductosVendidosHandlers;

namespace UsuarioControllers
{
    [ApiController]
    [Route("[controller]")]

    public class ProductoVendidoController : ControllerBase
    {
        [HttpGet(Name = "GetProductoVendido")]
        public List<ProductoVendido> ObtenerProductoVendido([FromHeader] int IDUsuario)
        {
            return ProductoVendidoHandler.BuscarProductosVendidos(IDUsuario);
        }
    }
}
